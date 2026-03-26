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

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <CollectionDomain.hpp>
#include <ConditionTrigger.hpp>
#include <Metric.hpp>
#include <Statistic.hpp>
#include <StatisticRegistry.hpp>

namespace NES
{
namespace
{

class StatisticRegistryTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("StatisticRegistryTest.log", LogLevel::LOG_DEBUG); }

    StatisticRegistry registry;

    static StatisticRegistry::Key makeKey(Metric metric, const std::string& source, const std::string& field, uint64_t windowMs)
    {
        return {
            .metric = metric,
            .collectionDomain = DataDomain{.logicalSourceName = source, .fieldName = field},
            .windowSize = Windowing::TimeMeasure{windowMs}};
    }

    static ConditionTrigger makeTrigger(const std::string& fieldName, const std::string& threshold)
    {
        const LogicalFunction condition{GreaterLogicalFunction{
            FieldAccessLogicalFunction{fieldName},
            ConstantValueLogicalFunction{
                DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE), threshold}}};
        return {.condition = condition, .callback = [](Statistic::StatisticId, Windowing::TimeMeasure, Windowing::TimeMeasure) { }};
    }

    static std::vector<ConditionTrigger> makeTriggers(size_t count)
    {
        std::vector<ConditionTrigger> triggers;
        triggers.reserve(count);
        for (size_t i = 0; i < count; ++i)
        {
            triggers.push_back(makeTrigger("value", std::to_string(i * 10)));
        }
        return triggers;
    }
};

TEST_F(StatisticRegistryTest, RegisterAndFind)
{
    const auto key = makeKey(Metric::Cardinality, "src", "field", 5000);
    const QueryId queryId{100};

    registry.registerStatistic(key, queryId, Statistic::StatisticId{42}, makeTriggers(2));
    auto result = registry.find(key);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->queryId, queryId);
    EXPECT_EQ(result->statisticId, Statistic::StatisticId{42});
    EXPECT_EQ(result->triggers.size(), 2);
}

TEST_F(StatisticRegistryTest, FindReturnsNulloptForUnknownKey)
{
    auto result = registry.find(makeKey(Metric::Cardinality, "unknown", "field", 5000));
    EXPECT_FALSE(result.has_value());
}

TEST_F(StatisticRegistryTest, FindReturnsNulloptForDifferentWindowSize)
{
    const auto key = makeKey(Metric::Cardinality, "src", "field", 5000);
    registry.registerStatistic(key, QueryId{100}, Statistic::StatisticId{42}, makeTriggers(0));

    auto differentWindow = makeKey(Metric::Cardinality, "src", "field", 10000);
    EXPECT_FALSE(registry.find(differentWindow).has_value());
}

TEST_F(StatisticRegistryTest, DifferentMetricsSameFieldAreSeparateEntries)
{
    const auto keyCardinality = makeKey(Metric::Cardinality, "src", "field", 5000);
    const auto keyMinVal = makeKey(Metric::MinVal, "src", "field", 5000);

    registry.registerStatistic(keyCardinality, QueryId{1}, Statistic::StatisticId{100}, makeTriggers(1));
    registry.registerStatistic(keyMinVal, QueryId{2}, Statistic::StatisticId{200}, makeTriggers(3));

    auto resultCardinality = registry.find(keyCardinality);
    auto resultMinVal = registry.find(keyMinVal);

    ASSERT_TRUE(resultCardinality.has_value());
    ASSERT_TRUE(resultMinVal.has_value());
    EXPECT_EQ(resultCardinality->queryId, QueryId{1});
    EXPECT_EQ(resultMinVal->queryId, QueryId{2});
}

TEST_F(StatisticRegistryTest, DeregisterRemovesEntry)
{
    const auto key = makeKey(Metric::Cardinality, "src", "field", 5000);
    registry.registerStatistic(key, QueryId{100}, Statistic::StatisticId{42}, makeTriggers(4));
    EXPECT_TRUE(registry.find(key).has_value());

    EXPECT_TRUE(registry.deregisterStatistic(key));
    EXPECT_FALSE(registry.find(key).has_value());
}

TEST_F(StatisticRegistryTest, DeregisterReturnsFalseForUnknownKey)
{
    EXPECT_FALSE(registry.deregisterStatistic(makeKey(Metric::Rate, "unknown", "field", 5000)));
}

TEST_F(StatisticRegistryTest, RegisterWithTriggers)
{
    const auto key = makeKey(Metric::Cardinality, "src", "field", 5000);
    const LogicalFunction condition{GreaterLogicalFunction{
        FieldAccessLogicalFunction{"value"},
        ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE), "100"}}};
    bool callbackInvoked = false;
    ConditionTrigger trigger{
        .condition = condition,
        .callback = [&](Statistic::StatisticId, Windowing::TimeMeasure, Windowing::TimeMeasure) { callbackInvoked = true; }};

    std::vector<ConditionTrigger> triggers;
    triggers.push_back(std::move(trigger));
    registry.registerStatistic(key, QueryId{100}, Statistic::StatisticId{42}, std::move(triggers));

    auto result = registry.find(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->triggers.size(), 1);
}

TEST_F(StatisticRegistryTest, AddTriggerToExistingEntry)
{
    const auto key = makeKey(Metric::Cardinality, "src", "field", 5000);
    registry.registerStatistic(key, QueryId{100}, Statistic::StatisticId{42}, makeTriggers(0));

    auto resultBefore = registry.find(key);
    ASSERT_TRUE(resultBefore.has_value());
    EXPECT_EQ(resultBefore->triggers.size(), 0);

    const LogicalFunction condition{GreaterLogicalFunction{
        FieldAccessLogicalFunction{"value"},
        ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE), "50"}}};
    ConditionTrigger trigger{
        .condition = condition, .callback = [](Statistic::StatisticId, Windowing::TimeMeasure, Windowing::TimeMeasure) { }};
    EXPECT_TRUE(registry.addTrigger(key, std::move(trigger)));

    auto resultAfter = registry.find(key);
    ASSERT_TRUE(resultAfter.has_value());
    EXPECT_EQ(resultAfter->triggers.size(), 1);
}

TEST_F(StatisticRegistryTest, AddTriggerToNonExistentKeyReturnsFalse)
{
    const LogicalFunction condition{GreaterLogicalFunction{
        FieldAccessLogicalFunction{"value"},
        ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE), "50"}}};
    ConditionTrigger trigger{
        .condition = condition, .callback = [](Statistic::StatisticId, Windowing::TimeMeasure, Windowing::TimeMeasure) { }};
    EXPECT_FALSE(registry.addTrigger(makeKey(Metric::Rate, "unknown", "field", 5000), std::move(trigger)));
}

}
}
