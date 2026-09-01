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

/// End-to-end cover for the statistic build and probe paths.
///
/// Everything else in this port is checked at compile time or in isolation; this is the only place a query is
/// actually compiled and run, so it is the only thing that exercises the Nautilus-traced code in
/// ScalarStatisticAggregationPhysicalFunction, StatisticStoreWriter and StatisticStoreReader.
///
/// There is no upstream harness to borrow: LogicalPlanBuilder has exactly one caller (the SQL parser) and no
/// existing test runs a programmatically built plan. Plans are therefore assembled by hand here, using the
/// anonymous source and sink overloads so that no catalog registration is needed.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DistributedLogicalPlan.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <ModelCatalog.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Statistic/ScalarStatisticProbeLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/ScalarStatisticAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStatus.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <StatisticStore/StatisticStoreRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{
namespace
{

const Host TEST_HOST{"localhost"};
constexpr uint64_t STATISTIC_ID = 401;
constexpr uint64_t WINDOW_SIZE_MS = 1000;

/// Two windows of four tuples each, so the averages are exact in FLOAT64 and distinguishable from one another:
/// [0, 1000) averages 20, [1000, 2000) averages 200.
constexpr std::string_view INPUT_CSV = "100,10\n200,20\n300,25\n400,25\n"
                                       "1100,100\n1200,200\n1300,250\n1400,250\n";

std::filesystem::path writeInput(const std::string& name)
{
    const auto path = std::filesystem::temp_directory_path() / name;
    std::ofstream out{path};
    out << INPUT_CSV;
    out.close();
    return path;
}

Schema<UnqualifiedUnboundField, Ordered> inputSchema()
{
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    return Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("ts"), uint64Type}, UnqualifiedUnboundField{Identifier::parse("value"), uint64Type}};
}

/// Source -> watermark -> windowed aggregation carrying a ScalarStatistic. The writer is fused into the
/// aggregation's lowering, so nothing about it appears here, and addWindowAggregation inserts the watermark
/// assigner itself.
LogicalPlan buildStatisticPlan(const std::filesystem::path& inputPath)
{
    auto plan = LogicalPlanBuilder::createLogicalPlan(
        Identifier::parse("File"),
        inputSchema(),
        {{Identifier::parse("FILE_PATH"), inputPath.string()}, {Identifier::parse("host"), std::string{TEST_HOST.getRawValue()}}},
        {{Identifier::parse("type"), "CSV"}});

    const Windowing::TimeBasedWindowType windowType{Windowing::TumblingWindow{Windowing::TimeMeasure{WINDOW_SIZE_MS}}};
    const Windowing::TimeCharacteristic timeCharacteristic{
        Windowing::UnboundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createEventTime(
            UnboundFieldAccessLogicalFunction{Identifier::parse("ts")}, Windowing::TimeUnit::Milliseconds())}};

    const ScalarStatisticAggregationLogicalFunction statisticFunction{
        TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{UnboundFieldAccessLogicalFunction{Identifier::parse("value")}},
        StatisticId{STATISTIC_ID},
        StatisticType::Avg};

    return LogicalPlanBuilder::addWindowAggregation(
        plan,
        windowType,
        {WindowedAggregationLogicalOperator::ProjectedAggregation{statisticFunction, statisticDataFieldName(StatisticId{STATISTIC_ID})}},
        {},
        timeCharacteristic);
}

LogicalPlan addFileSink(const LogicalPlan& plan, const std::filesystem::path& outputPath)
{
    return LogicalPlanBuilder::addAnonymousSink(
        Identifier::parse("File"),
        std::nullopt,
        {{Identifier::parse("FILE_PATH"), outputPath.string()},
         {Identifier::parse("OUTPUT_FORMAT"), "CSV"},
         {Identifier::parse("host"), std::string{TEST_HOST.getRawValue()}}},
        {},
        plan);
}

/// Optimizes a plan and runs it to completion on a single-node worker.
///
/// SingleNodeWorker::startQuery compiles the plan as given; it does not optimize. The optimizer is what resolves
/// the anonymous sink into a concrete one and stamps the traits the lowering rules read (MemoryLayoutTypeTrait,
/// FieldMappingTrait, OutputOriginIdsTrait), so it has to run first.
void runToCompletion(const LogicalPlan& plan)
{
    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    /// Operator placement needs somewhere to put the operators, and the source and sink carry a matching `host`.
    const auto workerCatalog = std::make_shared<WorkerCatalog>();
    workerCatalog->addWorker(TEST_HOST, "localhost:0", Capacity{CapacityKind::Unlimited{}}, {});
    const auto modelCatalog = std::make_shared<ModelCatalog>();

    const QueryOptimizer optimizer{QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog};
    const auto distributedPlan = optimizer.optimize(plan);
    ASSERT_EQ(distributedPlan.size(), 1U) << "single-node test expects exactly one local plan";
    const auto& localPlans = distributedPlan.begin()->second;
    ASSERT_EQ(localPlans.size(), 1U);

    const SingleNodeWorkerConfiguration configuration;
    SingleNodeWorker worker{configuration};
    const auto queryId = worker.startQuery(localPlans.front());
    ASSERT_TRUE(queryId.has_value()) << queryId.error().what();

    /// The File source terminates, so the query reaches a terminal state on its own.
    for (int attempt = 0; attempt < 200; ++attempt)
    {
        const auto status = worker.getQueryStatus(queryId.value());
        if (status.has_value() and status.value().state == QueryStatus::Stopped)
        {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    FAIL() << "query did not reach a terminal state";
}

std::string readFile(const std::filesystem::path& path)
{
    const std::ifstream in{path};
    std::ostringstream contents;
    contents << in.rdbuf();
    return contents.str();
}

}

class StatisticRoundTripTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("StatisticRoundTripTest.log", LogLevel::LOG_DEBUG); }

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

/// The build half: a windowed aggregation carrying a ScalarStatistic must persist one statistic per closed window.
/// If the writer's fusion into the aggregation lowering is wrong, or its traced code does not run, the store stays
/// empty and this fails.
TEST_F(StatisticRoundTripTest, ScalarStatisticAggregationPersistsOneStatisticPerWindow)
{
    const auto inputPath = writeInput("statistic-build-input.csv");
    const auto outputPath = std::filesystem::temp_directory_path() / "statistic-build-output.csv";
    std::filesystem::remove(outputPath);

    runToCompletion(addFileSink(buildStatisticPlan(inputPath), outputPath));

    const auto store = StatisticStoreRegistry::instance().getOrCreate(std::string{StatisticStoreRegistry::DEFAULT_STORE_NAME});
    const auto statistics
        = store->getStatistics(StatisticId{STATISTIC_ID}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{WINDOW_SIZE_MS * 10});

    ASSERT_EQ(statistics.size(), 2U) << "expected one statistic per closed window";
    for (const auto& statistic : statistics)
    {
        EXPECT_EQ(statistic.getStatisticType(), StatisticType::Avg);
        EXPECT_EQ(statistic.getNumberOfSeenTuples(), 4U);
        EXPECT_EQ(statistic.getStatisticDataSize(), sizeof(double));
    }
}

/// The read half, and the point of the whole port: a probe chained onto the build reads back what the writer just
/// stored. The two averages are distinct, so a value surviving the round trip cannot be a coincidence of
/// zero-initialised memory.
TEST_F(StatisticRoundTripTest, ProbeReadsBackTheStoredAverages)
{
    const auto inputPath = writeInput("statistic-probe-input.csv");
    const auto outputPath = std::filesystem::temp_directory_path() / "statistic-probe-output.csv";
    std::filesystem::remove(outputPath);

    auto plan = buildStatisticPlan(inputPath);

    /// The probe takes only the window bounds from the record; which statistic to read is operator state. The
    /// aggregation names its window fields "start"/"end" (Identifier uppercases them).
    ///
    /// Built childless and attached with withChildrenUnsafe, mirroring LogicalPlanBuilder::promoteOperatorToRoot.
    /// The child-taking constructor infers eagerly, but nothing in a freshly built plan has been inferred yet --
    /// that is the optimizer's TypeInferenceRule -- so inferring here would read an unset schema off the
    /// aggregation and abort.
    const auto probe = ScalarStatisticProbeLogicalOperator::create(
        StatisticId{STATISTIC_ID},
        StatisticType::Avg,
        DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE),
        Identifier::parse("start"),
        Identifier::parse("end"));
    plan = plan.withRootOperators({LogicalOperator{probe}.withChildrenUnsafe(plan.getRootOperators())});

    runToCompletion(addFileSink(plan, outputPath));

    ASSERT_TRUE(std::filesystem::exists(outputPath)) << "probe produced no sink output";
    const auto output = readFile(outputPath);

    /// Whole rows rather than substrings: "20" occurs inside "200", so a substring check would pass on the second
    /// window alone. Column order follows the probe's output schema, which is unordered, so it is pinned by the
    /// header the sink writes.
    EXPECT_NE(output.find("STATISTICEND:UINT64:NOT_NULLABLE,STATISTICID:UINT64:NOT_NULLABLE,"
                          "STATISTICNUMBEROFSEENTUPLES:UINT64:NOT_NULLABLE,STATISTICSTART:UINT64:NOT_NULLABLE,"
                          "STATISTICVALUE:FLOAT64:NOT_NULLABLE"),
              std::string::npos)
        << "unexpected probe output schema:\n"
        << output;
    EXPECT_NE(output.find("1000,401,4,0,20.0"), std::string::npos) << "first window [0,1000) avg 20 missing from:\n" << output;
    EXPECT_NE(output.find("2000,401,4,1000,200.0"), std::string::npos) << "second window [1000,2000) avg 200 missing from:\n" << output;
}

}
