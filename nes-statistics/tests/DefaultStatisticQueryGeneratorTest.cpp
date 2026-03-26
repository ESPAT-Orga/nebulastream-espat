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

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>
#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Operators/Windows/StatisticBuildLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <CollectionDomain.hpp>
#include <ConditionTrigger.hpp>
#include <DefaultStatisticQueryGenerator.hpp>
#include <ErrorHandling.hpp>
#include <Metric.hpp>
#include <RequestStatisticStatement.hpp>
#include <Statistic.hpp>

namespace NES
{
namespace
{

class DefaultStatisticQueryGeneratorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("DefaultStatisticQueryGeneratorTest.log", LogLevel::LOG_DEBUG); }

    DefaultStatisticQueryGenerator generator;

    /// Verifies that the generated plan contains exactly one InlineSinkLogicalOperator of type "Grpc"
    /// with the expected host and port in its config.
    static void assertGrpcSink(const LogicalPlan& plan, const std::string& expectedHost, const std::string& expectedPort)
    {
        const auto inlineSinks = getOperatorByType<InlineSinkLogicalOperator>(plan);
        ASSERT_EQ(inlineSinks.size(), 1);
        EXPECT_EQ(inlineSinks[0]->getSinkType(), "Grpc");
        const auto config = inlineSinks[0]->getSinkConfig();
        EXPECT_EQ(config.at("grpc_host"), expectedHost);
        EXPECT_EQ(config.at("grpc_port"), expectedPort);
    }
};

TEST_F(DefaultStatisticQueryGeneratorTest, GenerateCardinalityPlan)
{
    const RequestStatisticBuildStatement request{
        .domain = DataDomain{.logicalSourceName = "src", .fieldName = "field"},
        .metric = Metric::Cardinality,
        .windowSizeMs = 5000,
        .windowAdvanceMs = {},
        .eventTimeFieldName = {},
        .conditionTrigger = {},
        .options = {}};

    const auto plan = generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9001");

    const auto statisticBuilds = getOperatorByType<StatisticBuildLogicalOperator>(plan);
    ASSERT_EQ(statisticBuilds.size(), 1);

    const auto statisticWriters = getOperatorByType<StatisticStoreWriterLogicalOperator>(plan);
    ASSERT_EQ(statisticWriters.size(), 1);
    EXPECT_EQ(statisticWriters[0]->getStatisticType(), Statistic::StatisticType::Count_Min_Sketch);

    assertGrpcSink(plan, "localhost", "9001");
}

TEST_F(DefaultStatisticQueryGeneratorTest, GenerateMinValPlan)
{
    const RequestStatisticBuildStatement request{
        .domain = DataDomain{.logicalSourceName = "src", .fieldName = "field"},
        .metric = Metric::MinVal,
        .windowSizeMs = 5000,
        .windowAdvanceMs = {},
        .eventTimeFieldName = {},
        .conditionTrigger = {},
        .options = {}};

    const auto plan = generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9002");

    const auto statisticWriters = getOperatorByType<StatisticStoreWriterLogicalOperator>(plan);
    ASSERT_EQ(statisticWriters.size(), 1);
    EXPECT_EQ(statisticWriters[0]->getStatisticType(), Statistic::StatisticType::Equi_Width_Histogram);

    assertGrpcSink(plan, "localhost", "9002");
}

TEST_F(DefaultStatisticQueryGeneratorTest, GenerateAveragePlan)
{
    const RequestStatisticBuildStatement request{
        .domain = DataDomain{.logicalSourceName = "src", .fieldName = "field"},
        .metric = Metric::Average,
        .windowSizeMs = 10000,
        .windowAdvanceMs = {},
        .eventTimeFieldName = {},
        .conditionTrigger = {},
        .options = {}};

    const auto plan = generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9003");

    const auto statisticWriters = getOperatorByType<StatisticStoreWriterLogicalOperator>(plan);
    ASSERT_EQ(statisticWriters.size(), 1);
    EXPECT_EQ(statisticWriters[0]->getStatisticType(), Statistic::StatisticType::Reservoir_Sample);

    assertGrpcSink(plan, "localhost", "9003");
}

TEST_F(DefaultStatisticQueryGeneratorTest, OptionsOverrideDefaults)
{
    const RequestStatisticBuildStatement request{
        .domain = DataDomain{.logicalSourceName = "src", .fieldName = "field"},
        .metric = Metric::MinVal,
        .windowSizeMs = 5000,
        .windowAdvanceMs = {},
        .eventTimeFieldName = {},
        .conditionTrigger = {},
        .options = {{"buckets", "50"}, {"min", "0"}, {"max", "500"}}};

    const auto plan = generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9004");

    const auto statisticBuilds = getOperatorByType<StatisticBuildLogicalOperator>(plan);
    ASSERT_EQ(statisticBuilds.size(), 1);

    assertGrpcSink(plan, "localhost", "9004");
}

TEST_F(DefaultStatisticQueryGeneratorTest, IngestionTimeIsDefaultWhenNoEventTimeField)
{
    const RequestStatisticBuildStatement request{
        .domain = DataDomain{.logicalSourceName = "src", .fieldName = "field"},
        .metric = Metric::Cardinality,
        .windowSizeMs = 5000,
        .windowAdvanceMs = {},
        .eventTimeFieldName = {},
        .conditionTrigger = {},
        .options = {}};

    const auto plan = generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9005");

    const auto ingestionWatermarks = getOperatorByType<IngestionTimeWatermarkAssignerLogicalOperator>(plan);
    EXPECT_EQ(ingestionWatermarks.size(), 1);
    const auto eventWatermarks = getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan);
    EXPECT_EQ(eventWatermarks.size(), 0);

    assertGrpcSink(plan, "localhost", "9005");
}

TEST_F(DefaultStatisticQueryGeneratorTest, EventTimeUsedWhenFieldSpecified)
{
    const RequestStatisticBuildStatement request{
        .domain = DataDomain{.logicalSourceName = "src", .fieldName = "field"},
        .metric = Metric::Cardinality,
        .windowSizeMs = 5000,
        .windowAdvanceMs = {},
        .eventTimeFieldName = "timestamp",
        .conditionTrigger = {},
        .options = {}};

    const auto plan = generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9006");

    const auto eventWatermarks = getOperatorByType<EventTimeWatermarkAssignerLogicalOperator>(plan);
    EXPECT_EQ(eventWatermarks.size(), 1);
    const auto ingestionWatermarks = getOperatorByType<IngestionTimeWatermarkAssignerLogicalOperator>(plan);
    EXPECT_EQ(ingestionWatermarks.size(), 0);

    assertGrpcSink(plan, "localhost", "9006");
}

TEST_F(DefaultStatisticQueryGeneratorTest, WorkloadDomainThrowsNotYetImplemented)
{
    const RequestStatisticBuildStatement request{
        .domain = WorkloadDomain{.queryId = QueryId{42}, .operatorId = OperatorId{5}, .fieldName = "field"},
        .metric = Metric::Cardinality,
        .windowSizeMs = 5000,
        .windowAdvanceMs = {},
        .eventTimeFieldName = {},
        .conditionTrigger = {},
        .options = {}};

    ASSERT_EXCEPTION_ERRORCODE(
        (void)generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9007"), ErrorCode::NotImplemented);
}

TEST_F(DefaultStatisticQueryGeneratorTest, InfrastructureDomainThrowsNotYetImplemented)
{
    const RequestStatisticBuildStatement request{
        .domain = InfrastructureDomain{WorkerId{"worker1"}},
        .metric = Metric::Rate,
        .windowSizeMs = 5000,
        .windowAdvanceMs = {},
        .eventTimeFieldName = {},
        .conditionTrigger = {},
        .options = {}};

    ASSERT_EXCEPTION_ERRORCODE(
        (void)generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9008"), ErrorCode::NotImplemented);
}

TEST_F(DefaultStatisticQueryGeneratorTest, GeneratePlanWithCondition)
{
    const RequestStatisticBuildStatement
        request{
            .domain = DataDomain{.logicalSourceName = "src", .fieldName = "field"},
            .metric = Metric::Cardinality,
            .windowSizeMs = 5000,
            .windowAdvanceMs = {},
            .eventTimeFieldName = {},
            .conditionTrigger = ConditionTrigger{.condition = LogicalFunction{GreaterLogicalFunction{FieldAccessLogicalFunction{"value"}, ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE), "100"}}}, .callback = [](Statistic::StatisticId, Windowing::TimeMeasure, Windowing::TimeMeasure) { }},
            .options = {}};

    const auto plan = generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9009");

    const auto selections = getOperatorByType<SelectionLogicalOperator>(plan);
    ASSERT_EQ(selections.size(), 1);

    const auto statisticWriters = getOperatorByType<StatisticStoreWriterLogicalOperator>(plan);
    ASSERT_EQ(statisticWriters.size(), 1);

    assertGrpcSink(plan, "localhost", "9009");
}

TEST_F(DefaultStatisticQueryGeneratorTest, GeneratePlanWithoutConditionHasNoSelection)
{
    const RequestStatisticBuildStatement request{
        .domain = DataDomain{.logicalSourceName = "src", .fieldName = "field"},
        .metric = Metric::Cardinality,
        .windowSizeMs = 5000,
        .windowAdvanceMs = {},
        .eventTimeFieldName = {},
        .conditionTrigger = {},
        .options = {}};

    const auto plan = generator.generateQuery(request, Statistic::StatisticId{1}, "localhost:9010");

    const auto selections = getOperatorByType<SelectionLogicalOperator>(plan);
    EXPECT_EQ(selections.size(), 0);

    assertGrpcSink(plan, "localhost", "9010");
}

}
}
