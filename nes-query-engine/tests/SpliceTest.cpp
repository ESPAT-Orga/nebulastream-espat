/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngineTestingInfrastructure.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <TestSource.hpp>

/// Regression tests for the SpliceToRunningSource deployment race (RunningSourceRegistry deferred
/// splice queue). A statistic-build branch query has no source of its own: at pipeline-setup
/// completion it grafts its pipelines onto the data query's already-running source, looked up by
/// logical source name. These tests drive everything in memory (TestSource / TestSink) — no CSV
/// input, no ENABLE_LARGE_TESTS — and verify that deployment order no longer matters:
///   - the build branch deployed BEFORE the data query (the bug) no longer hard-fails; and
///   - data emitted by the data source reaches the grafted build branch's sink.
namespace NES::Testing
{

namespace
{
constexpr std::string_view LOGICAL_SOURCE = "BID";

/// Build a data query: one deferred-start source registered under LOGICAL_SOURCE that waits for
/// `expectedSplices` build branches to graft on before it begins emitting. Returns the plan plus
/// the source and sink identifiers (to reach their controllers via the harness).
std::tuple<std::unique_ptr<ExecutableQueryPlan>, QueryPlanBuilder::identifier_t, QueryPlanBuilder::identifier_t>
makeDataQuery(TestingHarness& test, uint32_t expectedSplices)
{
    auto builder = test.buildNewQuery();
    auto source = builder.addSource(QueryPlanBuilder::SourceConfig{
        .deferStart = true, .deferStartExpectedSpliceCount = expectedSplices, .logicalSourceName = std::string{LOGICAL_SOURCE}});
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));
    return {std::move(query), source, sink};
}

/// Build a statistic-build-branch query: a splice source (no thread of its own) that grafts onto
/// the LOGICAL_SOURCE running source. Returns the plan plus the sink identifier.
std::tuple<std::unique_ptr<ExecutableQueryPlan>, QueryPlanBuilder::identifier_t> makeBuildBranch(TestingHarness& test)
{
    auto builder = test.buildNewQuery();
    auto source = builder.addSource(
        QueryPlanBuilder::SourceConfig{.spliceToRunningSource = true, .logicalSourceName = std::string{LOGICAL_SOURCE}});
    auto sink = builder.addSink({builder.addPipeline({source})});
    auto query = test.addNewQuery(std::move(builder));
    return {std::move(query), sink};
}
}

class SpliceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SpliceTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SpliceTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

/// The data query is deployed first: its source registers (deferred), then the build branch
/// splices in, the splice budget hits 0 and the source starts. Data reaches both sinks.
TEST_F(SpliceTest, registerThenSplice)
{
    TestingHarness test;
    auto [dataQuery, dataSource, dataSink] = makeDataQuery(test, 1);
    auto [buildQuery, buildSink] = makeBuildBranch(test);
    const auto dataId = dataQuery->queryId;
    const auto buildId = buildQuery->queryId;
    auto dataCtrl = test.sourceControls[dataSource];

    test.expectQueryStatusEvents(dataId, {QueryStatus::Started, QueryStatus::Running});
    test.expectQueryStatusEvents(buildId, {QueryStatus::Started, QueryStatus::Running});

    test.start();
    {
        test.startQuery(std::move(dataQuery));
        test.startQuery(std::move(buildQuery));

        /// The deferred source only opens once the single expected splice has grafted on.
        ASSERT_TRUE(dataCtrl->waitUntilOpened());

        for (int i = 0; i < 4; ++i)
        {
            dataCtrl->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER);
        }

        ASSERT_TRUE(test.sinkControls[dataSink]->waitForNumberOfReceivedBuffersOrMore(1));
        /// The grafted build branch must receive the same data the source emits.
        ASSERT_TRUE(test.sinkControls[buildSink]->waitForNumberOfReceivedBuffersOrMore(1));

        ASSERT_TRUE(test.waitForQepRunning(dataId, DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.waitForQepRunning(buildId, DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();
}

/// Regression for the bug: the build branch is deployed BEFORE the data query. The old code
/// hard-failed the build branch ("no running source registered"); now the splice is queued and
/// applied when the data source registers, so both queries run and data reaches both sinks.
TEST_F(SpliceTest, spliceThenRegister)
{
    TestingHarness test;
    auto [dataQuery, dataSource, dataSink] = makeDataQuery(test, 1);
    auto [buildQuery, buildSink] = makeBuildBranch(test);
    const auto dataId = dataQuery->queryId;
    const auto buildId = buildQuery->queryId;
    auto dataCtrl = test.sourceControls[dataSource];

    test.expectQueryStatusEvents(dataId, {QueryStatus::Started, QueryStatus::Running});
    test.expectQueryStatusEvents(buildId, {QueryStatus::Started, QueryStatus::Running});

    test.start();
    {
        /// Build branch first: its splice finds no source yet and must be QUEUED, not failed.
        test.startQuery(std::move(buildQuery));
        test.startQuery(std::move(dataQuery));

        /// Registration drains the queued splice, the budget hits 0 and the source opens.
        ASSERT_TRUE(dataCtrl->waitUntilOpened());

        for (int i = 0; i < 4; ++i)
        {
            dataCtrl->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER);
        }

        ASSERT_TRUE(test.sinkControls[dataSink]->waitForNumberOfReceivedBuffersOrMore(1));
        ASSERT_TRUE(test.sinkControls[buildSink]->waitForNumberOfReceivedBuffersOrMore(1));

        ASSERT_TRUE(test.waitForQepRunning(dataId, DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.waitForQepRunning(buildId, DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();
}

/// The deferred-start budget counts every splice: with expectedSpliceCount == 2 the source must
/// start only after BOTH build branches graft on. Verified indirectly — if the countdown were
/// wrong the source would never emit and no sink would receive data.
TEST_F(SpliceTest, twoSplicesCountdown)
{
    TestingHarness test;
    auto [dataQuery, dataSource, dataSink] = makeDataQuery(test, 2);
    auto [buildOne, buildOneSink] = makeBuildBranch(test);
    auto [buildTwo, buildTwoSink] = makeBuildBranch(test);
    const auto dataId = dataQuery->queryId;
    const auto buildOneId = buildOne->queryId;
    const auto buildTwoId = buildTwo->queryId;
    auto dataCtrl = test.sourceControls[dataSource];

    test.expectQueryStatusEvents(dataId, {QueryStatus::Started, QueryStatus::Running});
    test.expectQueryStatusEvents(buildOneId, {QueryStatus::Started, QueryStatus::Running});
    test.expectQueryStatusEvents(buildTwoId, {QueryStatus::Started, QueryStatus::Running});

    test.start();
    {
        test.startQuery(std::move(dataQuery));
        test.startQuery(std::move(buildOne));
        test.startQuery(std::move(buildTwo));

        ASSERT_TRUE(dataCtrl->waitUntilOpened());

        for (int i = 0; i < 4; ++i)
        {
            dataCtrl->injectData(identifiableData(1), NUMBER_OF_TUPLES_PER_BUFFER);
        }

        ASSERT_TRUE(test.sinkControls[dataSink]->waitForNumberOfReceivedBuffersOrMore(1));
        ASSERT_TRUE(test.sinkControls[buildOneSink]->waitForNumberOfReceivedBuffersOrMore(1));
        ASSERT_TRUE(test.sinkControls[buildTwoSink]->waitForNumberOfReceivedBuffersOrMore(1));

        ASSERT_TRUE(test.waitForQepRunning(dataId, DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.waitForQepRunning(buildOneId, DEFAULT_LONG_AWAIT_TIMEOUT));
        ASSERT_TRUE(test.waitForQepRunning(buildTwoId, DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();
}

/// A build branch whose data query never arrives must NOT hard-fail: its splice stays queued and
/// the query still reaches Running. Engine shutdown then tears it down cleanly. (Old code failed
/// the query immediately.)
TEST_F(SpliceTest, spliceWithoutDataQueryIdlesWithoutFailing)
{
    TestingHarness test;
    auto [buildQuery, buildSink] = makeBuildBranch(test);
    const auto buildId = buildQuery->queryId;

    test.expectQueryStatusEvents(buildId, {QueryStatus::Started, QueryStatus::Running});

    test.start();
    {
        test.startQuery(std::move(buildQuery));
        /// Reaches Running with the splice queued; no data flows because no source registers.
        ASSERT_TRUE(test.waitForQepRunning(buildId, DEFAULT_LONG_AWAIT_TIMEOUT));
    }
    test.stop();
}

}
