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

#include <functional>
#include <string>
#include <DataTypes/DataType.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/ScalarStatisticAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Util/PlanRenderer.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{

AggregationFieldAccess fieldAccess(const std::string& name)
{
    return TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{UnboundFieldAccessLogicalFunction{Identifier::parse(name)}};
}

ScalarStatisticAggregationLogicalFunction makeFunction(
    const StatisticType op = StatisticType::Avg, const uint64_t statisticId = 401, const std::string& field = "value")
{
    return ScalarStatisticAggregationLogicalFunction{fieldAccess(field), StatisticId{statisticId}, op};
}

}

class ScalarStatisticAggregationLogicalFunctionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("ScalarStatisticAggregationLogicalFunctionTest.log", LogLevel::LOG_DEBUG); }
};

/// The reason this function has to exist at all: StatisticStoreWriter reads its payload field with
/// getRawValueAs<VariableSizedData>(), so a plain scalar aggregate cannot feed it.
TEST_F(ScalarStatisticAggregationLogicalFunctionTest, AggregateTypeIsVarsizedAndNotNullable)
{
    for (const auto op : {StatisticType::Count, StatisticType::Sum, StatisticType::Avg})
    {
        const auto aggregateType = makeFunction(op).getAggregateType();
        EXPECT_TRUE(aggregateType.isType(DataType::Type::VARSIZED)) << "op " << static_cast<int>(op);
        EXPECT_FALSE(aggregateType.nullable) << "op " << static_cast<int>(op);
    }
}

TEST_F(ScalarStatisticAggregationLogicalFunctionTest, AllThreeOpsShareOnePluginName)
{
    EXPECT_EQ(ScalarStatisticAggregationLogicalFunction::getName(), "ScalarStatistic");
}

TEST_F(ScalarStatisticAggregationLogicalFunctionTest, NullValuesAreExcluded)
{
    EXPECT_FALSE(ScalarStatisticAggregationLogicalFunction::shallIncludeNullValues());
}

TEST_F(ScalarStatisticAggregationLogicalFunctionTest, CarriesStatisticIdAndOp)
{
    const auto function = makeFunction(StatisticType::Sum, 7);
    EXPECT_EQ(function.getStatisticId(), StatisticId{7});
    EXPECT_EQ(function.getOp(), StatisticType::Sum);
}

/// The id and the op are part of identity: two statistics over the same field are still different statistics.
TEST_F(ScalarStatisticAggregationLogicalFunctionTest, EqualityDistinguishesIdOpAndField)
{
    const auto base = makeFunction(StatisticType::Avg, 1, "value");

    EXPECT_EQ(base, makeFunction(StatisticType::Avg, 1, "value"));
    EXPECT_FALSE(base == makeFunction(StatisticType::Sum, 1, "value"));
    EXPECT_FALSE(base == makeFunction(StatisticType::Avg, 2, "value"));
    EXPECT_FALSE(base == makeFunction(StatisticType::Avg, 1, "other"));
}

TEST_F(ScalarStatisticAggregationLogicalFunctionTest, EqualFunctionsHashEqually)
{
    const std::hash<ScalarStatisticAggregationLogicalFunction> hash;
    EXPECT_EQ(hash(makeFunction(StatisticType::Avg, 1)), hash(makeFunction(StatisticType::Avg, 1)));
    EXPECT_NE(hash(makeFunction(StatisticType::Avg, 1)), hash(makeFunction(StatisticType::Avg, 2)));
}

TEST_F(ScalarStatisticAggregationLogicalFunctionTest, ExplainNamesTheOp)
{
    const auto explained = makeFunction(StatisticType::Sum).explain(ExplainVerbosity::Debug);
    EXPECT_NE(explained.find("ScalarStatistic"), std::string::npos) << explained;
    EXPECT_NE(explained.find("Sum"), std::string::npos) << explained;
}

/// The writer derives the payload field name from the id alone, so producer and consumer cannot drift.
TEST_F(ScalarStatisticAggregationLogicalFunctionTest, PayloadFieldNameIsDerivedFromTheId)
{
    EXPECT_EQ(statisticDataFieldName(StatisticId{401}), Identifier::parse("STATISTICDATA_401"));
    EXPECT_NE(statisticDataFieldName(StatisticId{401}), statisticDataFieldName(StatisticId{402}));
}

}
