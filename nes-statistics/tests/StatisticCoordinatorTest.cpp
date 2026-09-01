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

/// The coordinator driving the whole thing: collectNewStatistic deploys a build query that reports back over
/// gRPC, and getStatistics deploys a probe query, impulses it, and collects the answers.
///
/// Unlike the other suites this uses a named logical source, because that is what a real request names, so it
/// registers one in a SourceCatalog and hands the same catalog to the optimizer.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <CollectionDomain.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DefaultStatisticQueryGenerator.hpp>
#include <DistributedLogicalPlan.hpp>
#include <Identifiers/Identifier.hpp>
#include <Metric.hpp>
#include <ModelCatalog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStatus.hpp>
#include <RequestStatisticStatement.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <StatisticCoordinator.hpp>
#include <StatisticRegistry.hpp>
#include <StatisticStore/StatisticStoreRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <StatisticTestSupport.hpp>

namespace NES
{
namespace
{

using namespace StatisticTestSupport;

constexpr auto SOURCE_NAME = "teststream";

/// Owns the worker, the catalogs and the optimizer, and exposes the SubmitQueryFn the coordinator needs.
///
/// Query ids are recorded so a test can wait for the build query to finish before probing: the probe reads what
/// the build wrote, so the two must not overlap.
class TestSubmissionBackend
{
public:
    explicit TestSubmissionBackend(const std::filesystem::path& inputPath)
        : sourceCatalog(std::make_shared<SourceCatalog>())
        , sinkCatalog(std::make_shared<SinkCatalog>())
        , workerCatalog(std::make_shared<WorkerCatalog>())
        , modelCatalog(std::make_shared<ModelCatalog>())
    {
        workerCatalog->addWorker(TEST_HOST, "localhost:0", Capacity{CapacityKind::Unlimited{}}, {});

        const auto logicalSource = sourceCatalog->addLogicalSource(Identifier::parse(SOURCE_NAME), inputSchema());
        if (logicalSource.has_value())
        {
            const auto physical = sourceCatalog->addPhysicalSource(
                *logicalSource,
                Identifier::parse("File"),
                TEST_HOST,
                {{Identifier::parse("FILE_PATH"), inputPath.string()}},
                {{Identifier::parse("type"), "CSV"}});
            (void)physical;
        }

        optimizer = std::make_unique<QueryOptimizer>(
            QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog);
        worker = std::make_unique<SingleNodeWorker>(SingleNodeWorkerConfiguration{});
    }

    [[nodiscard]] StatisticCoordinator::SubmitQueryFn submitFn()
    {
        return [this](LogicalPlan plan) -> std::expected<QueryId, Exception>
        {
            const auto distributed = optimizer->optimize(std::move(plan));
            if (distributed.size() != 1)
            {
                return std::unexpected(QueryStartFailed("expected exactly one local plan, got {}", distributed.size()));
            }
            const auto& localPlans = distributed.begin()->second;
            if (localPlans.size() != 1)
            {
                return std::unexpected(QueryStartFailed("expected exactly one local plan for the worker"));
            }
            auto queryId = worker->startQuery(localPlans.front());
            if (queryId.has_value())
            {
                const std::lock_guard lock(mutex);
                submitted.push_back(queryId.value());
            }
            return queryId;
        };
    }

    /// Waits until every query submitted so far has stopped. The build query reads a file, so it terminates.
    [[nodiscard]] bool waitForAllStopped(const std::chrono::milliseconds timeout) const
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            bool allStopped = true;
            {
                const std::lock_guard lock(mutex);
                for (const auto& queryId : submitted)
                {
                    const auto status = worker->getQueryStatus(queryId);
                    if (not status.has_value() or status.value().state != QueryStatus::Stopped)
                    {
                        allStopped = false;
                        break;
                    }
                }
            }
            if (allStopped)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
        return false;
    }

private:
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
    std::shared_ptr<WorkerCatalog> workerCatalog;
    std::shared_ptr<ModelCatalog> modelCatalog;
    std::unique_ptr<QueryOptimizer> optimizer;
    std::unique_ptr<SingleNodeWorker> worker;

    mutable std::mutex mutex;
    std::vector<QueryId> submitted;
};

RequestStatisticBuildStatement averageOverValue()
{
    return RequestStatisticBuildStatement{
        .domain = DataDomain{.logicalSourceName = SOURCE_NAME, .fieldName = "value"},
        .metric = Metric::Average,
        .windowSizeMs = WINDOW_SIZE_MS,
        .windowAdvanceMs = std::nullopt,
        .eventTimeFieldName = "ts",
        .conditionTrigger = std::nullopt,
        .options = {}};
}

StatisticRegistry::Key keyFor(const RequestStatisticBuildStatement& statement)
{
    return StatisticRegistry::Key{
        .metric = statement.metric, .collectionDomain = statement.domain, .windowSize = Windowing::TimeMeasure{statement.windowSizeMs}};
}

}

class StatisticCoordinatorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("StatisticCoordinatorTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        StatisticStoreRegistry::instance().clear();
    }

    void TearDown() override
    {
        StatisticStoreRegistry::instance().clear();
        BaseUnitTest::TearDown();
    }
};

/// The build half through the coordinator: a request turns into a deployed query whose results reach
/// onStatisticReport over gRPC, and whose statistics land in the store.
TEST_F(StatisticCoordinatorTest, CollectNewStatisticDeploysAQueryThatReportsBack)
{
    const auto inputPath = writeInput("coordinator-collect-input.csv");
    TestSubmissionBackend backend{inputPath};

    StatisticCoordinator coordinator{std::make_unique<DefaultStatisticQueryGenerator>(), backend.submitFn()};
    const auto address = coordinator.startGrpcServer();
    ASSERT_FALSE(address.empty());

    const auto statement = averageOverValue();
    const auto result = coordinator.collectNewStatistic(statement);
    ASSERT_TRUE(result.has_value()) << result.error().what();
    EXPECT_FALSE(result->alreadyExisted);

    ASSERT_TRUE(backend.waitForAllStopped(std::chrono::seconds{60})) << "the build query never finished";

    const auto store = StatisticStoreRegistry::instance().getOrCreate(std::string{StatisticStoreRegistry::DEFAULT_STORE_NAME});
    const auto statistics = store->getStatistics(
        result->statisticId, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{WINDOW_SIZE_MS * 10});
    EXPECT_EQ(statistics.size(), 2U) << "expected one statistic per closed window";
}

/// A second identical request must not deploy a second query.
TEST_F(StatisticCoordinatorTest, IdenticalRequestsAreDeduplicated)
{
    const auto inputPath = writeInput("coordinator-dedup-input.csv");
    TestSubmissionBackend backend{inputPath};

    StatisticCoordinator coordinator{std::make_unique<DefaultStatisticQueryGenerator>(), backend.submitFn()};
    coordinator.startGrpcServer();

    const auto statement = averageOverValue();
    const auto first = coordinator.collectNewStatistic(statement);
    ASSERT_TRUE(first.has_value()) << first.error().what();
    const auto second = coordinator.collectNewStatistic(statement);
    ASSERT_TRUE(second.has_value()) << second.error().what();

    EXPECT_TRUE(second->alreadyExisted);
    EXPECT_EQ(first->statisticId, second->statisticId);
    EXPECT_EQ(first->queryId, second->queryId);
}

/// The read half through the coordinator: after a statistic has been built, getStatistics deploys the probe,
/// impulses it, and returns what the probe read back out of the store. The two windows average 20 and 200, so a
/// correct sum is 220.
TEST_F(StatisticCoordinatorTest, GetStatisticsProbesTheCollectedStatistic)
{
    const auto inputPath = writeInput("coordinator-probe-input.csv");
    TestSubmissionBackend backend{inputPath};

    StatisticCoordinator coordinator{std::make_unique<DefaultStatisticQueryGenerator>(), backend.submitFn()};
    coordinator.startGrpcServer();

    const auto statement = averageOverValue();
    const auto collected = coordinator.collectNewStatistic(statement);
    ASSERT_TRUE(collected.has_value()) << collected.error().what();

    /// The probe reads what the build wrote, so the build has to be done first.
    ASSERT_TRUE(backend.waitForAllStopped(std::chrono::seconds{60})) << "the build query never finished";

    const auto probed = coordinator.getStatistics(
        {keyFor(statement)}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{WINDOW_SIZE_MS * 10});

    ASSERT_TRUE(probed.has_value()) << "no report came back before the timeout";
    EXPECT_DOUBLE_EQ(probed.value(), 220.0) << "expected the sum of both windows' averages";
}

/// The domains and metrics outside this port's slice keep their interface and fail cleanly.
TEST_F(StatisticCoordinatorTest, UnsupportedRequestsFailWithNotImplemented)
{
    const auto inputPath = writeInput("coordinator-unsupported-input.csv");
    TestSubmissionBackend backend{inputPath};

    StatisticCoordinator coordinator{std::make_unique<DefaultStatisticQueryGenerator>(), backend.submitFn()};
    coordinator.startGrpcServer();

    auto infrastructure = averageOverValue();
    infrastructure.domain = InfrastructureDomain{Host{"worker1"}};
    const auto infrastructureResult = coordinator.collectNewStatistic(infrastructure);
    ASSERT_FALSE(infrastructureResult.has_value());
    EXPECT_EQ(infrastructureResult.error().code(), ErrorCode::NotImplemented);

    auto workload = averageOverValue();
    workload.domain = WorkloadDomain{.queryId = QueryId::invalid(), .operatorId = OperatorId{1}, .fieldName = "value"};
    const auto workloadResult = coordinator.collectNewStatistic(workload);
    ASSERT_FALSE(workloadResult.has_value());
    EXPECT_EQ(workloadResult.error().code(), ErrorCode::NotImplemented);

    auto unsupportedMetric = averageOverValue();
    unsupportedMetric.metric = Metric::Cardinality;
    const auto metricResult = coordinator.collectNewStatistic(unsupportedMetric);
    ASSERT_FALSE(metricResult.has_value());
    EXPECT_EQ(metricResult.error().code(), ErrorCode::NotImplemented);
}

}
