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
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Operators/Statistic/StatisticTargetUtil.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Scalar/ScalarStatisticLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/StatisticBuildLogicalOperator.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Statistic.hpp>

namespace NES
{

/// Round-trip tests for the multi-synopsis StatisticBuild + chained StatisticStoreWriter design.
/// The contract that must survive (de)serialization: a StatisticBuild keeps all N aggregations with their
/// distinct statisticIds and types, and each per-synopsis VARSIZED data-field name is re-derived from the id
/// (statisticDataFieldName), so producers (build) and consumers (writers) cannot drift even though only the id
/// is ever serialized.
class StatisticOperatorsReflectionTest : public ::testing::Test
{
protected:
    static constexpr uint64_t MemoryBudget = 288;

    static std::shared_ptr<WindowAggregationLogicalFunction> countMin(const std::string& field, const uint64_t id)
    {
        return std::make_shared<WindowAggregationLogicalFunction>(
            CountMinSketchLogicalFunction{FieldAccessLogicalFunction(field), MemoryBudget, Statistic::StatisticId{id}});
    }

    static std::shared_ptr<WindowAggregationLogicalFunction> histogram(const std::string& field, const uint64_t id)
    {
        return std::make_shared<WindowAggregationLogicalFunction>(EquiWidthHistogramLogicalFunction{
            FieldAccessLogicalFunction(field), MemoryBudget, /*minValue*/ 0, /*maxValue*/ 1000, Statistic::StatisticId{id}});
    }

    static std::shared_ptr<WindowAggregationLogicalFunction>
    scalar(const std::string& field, const uint64_t id, const Statistic::StatisticType op)
    {
        return std::make_shared<WindowAggregationLogicalFunction>(ScalarStatisticLogicalFunction{
            FieldAccessLogicalFunction(field), FieldAccessLogicalFunction(field), Statistic::StatisticId{id}, op});
    }

    static std::shared_ptr<Windowing::WindowType> tumblingWindow()
    {
        return std::make_shared<Windowing::TumblingWindow>(
            Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(1000));
    }
};

/// Several CountMin sketches over distinct fields/ids must all survive reflect -> unreflect with their ids
/// intact, distinct, and their derived data-field names re-deriving to the same strings.
TEST_F(StatisticOperatorsReflectionTest, MultiCountMinStatisticBuildSurvivesReflectionRoundTrip)
{
    const std::vector<uint64_t> ids = {11, 22, 33};
    const StatisticBuildLogicalOperator op(
        {countMin("stream.a", ids[0]), countMin("stream.b", ids[1]), countMin("stream.c", ids[2])},
        tumblingWindow(),
        std::make_shared<LogicalStatisticFields>());

    const auto roundTripped = unreflect<StatisticBuildLogicalOperator>(NES::reflect(op));

    const auto aggregations = roundTripped.getWindowAggregation();
    ASSERT_EQ(aggregations.size(), ids.size());

    std::unordered_set<Statistic::StatisticId::Underlying> seenIds;
    for (size_t i = 0; i < aggregations.size(); ++i)
    {
        const auto target = tryGetStatisticTarget(*aggregations[i]);
        ASSERT_TRUE(target.has_value());
        EXPECT_EQ(target->statisticId, Statistic::StatisticId{ids[i]});
        EXPECT_EQ(target->statisticType, Statistic::StatisticType::Count_Min_Sketch);
        /// The data-field name is re-derived from the id, so it must match the original id's name.
        EXPECT_EQ(statisticDataFieldName(target->statisticId), statisticDataFieldName(Statistic::StatisticId{ids[i]}));
        seenIds.insert(target->statisticId.getRawValue());
    }
    EXPECT_EQ(seenIds.size(), ids.size()) << "statisticIds must stay distinct after the round trip";
}

/// A build mixing synopsis kinds must preserve each aggregation's TYPE as well as its id, proving the type is
/// not inferred from position or a shared field but recovered per-aggregation.
TEST_F(StatisticOperatorsReflectionTest, MixedSynopsisStatisticBuildSurvivesReflectionRoundTrip)
{
    const StatisticBuildLogicalOperator op(
        {countMin("stream.a", 101), histogram("stream.b", 202), countMin("stream.c", 303)},
        tumblingWindow(),
        std::make_shared<LogicalStatisticFields>());

    const auto roundTripped = unreflect<StatisticBuildLogicalOperator>(NES::reflect(op));

    const auto aggregations = roundTripped.getWindowAggregation();
    ASSERT_EQ(aggregations.size(), 3u);

    const std::vector<std::pair<uint64_t, Statistic::StatisticType>> expected = {
        {101, Statistic::StatisticType::Count_Min_Sketch},
        {202, Statistic::StatisticType::Equi_Width_Histogram},
        {303, Statistic::StatisticType::Count_Min_Sketch},
    };
    for (size_t i = 0; i < aggregations.size(); ++i)
    {
        const auto target = tryGetStatisticTarget(*aggregations[i]);
        ASSERT_TRUE(target.has_value());
        EXPECT_EQ(target->statisticId, Statistic::StatisticId{expected[i].first});
        EXPECT_EQ(target->statisticType, expected[i].second);
    }
}

/// The scalar statistics (Count / Sum / Avg) share one logical class distinguished by an `op` field; that op
/// selects the StatisticType and must survive the reflect -> unreflect round trip alongside the statisticId.
TEST_F(StatisticOperatorsReflectionTest, ScalarStatisticBuildSurvivesReflectionRoundTrip)
{
    const std::vector<std::pair<uint64_t, Statistic::StatisticType>> expected = {
        {601, Statistic::StatisticType::Count},
        {602, Statistic::StatisticType::Sum},
        {603, Statistic::StatisticType::Avg},
    };
    const StatisticBuildLogicalOperator op(
        {scalar("stream.a", expected[0].first, expected[0].second),
         scalar("stream.b", expected[1].first, expected[1].second),
         scalar("stream.c", expected[2].first, expected[2].second)},
        tumblingWindow(),
        std::make_shared<LogicalStatisticFields>());

    const auto roundTripped = unreflect<StatisticBuildLogicalOperator>(NES::reflect(op));

    const auto aggregations = roundTripped.getWindowAggregation();
    ASSERT_EQ(aggregations.size(), expected.size());
    for (size_t i = 0; i < aggregations.size(); ++i)
    {
        const auto target = tryGetStatisticTarget(*aggregations[i]);
        ASSERT_TRUE(target.has_value());
        EXPECT_EQ(target->statisticId, Statistic::StatisticId{expected[i].first});
        EXPECT_EQ(target->statisticType, expected[i].second);
    }
}

/// Each chained single-target StatisticStoreWriter must round-trip its (statisticId, statisticType). The data
/// field name is intentionally NOT serialized; it is re-derived from the id, so checking id + type is enough.
TEST_F(StatisticOperatorsReflectionTest, ChainedStatisticStoreWritersSurviveReflectionRoundTrip)
{
    const std::vector<StatisticTarget> targets = {
        {Statistic::StatisticId{11}, Statistic::StatisticType::Count_Min_Sketch},
        {Statistic::StatisticId{22}, Statistic::StatisticType::Equi_Width_Histogram},
        {Statistic::StatisticId{33}, Statistic::StatisticType::Reservoir_Sample},
    };

    for (const auto& target : targets)
    {
        const StatisticStoreWriterLogicalOperator writer(
            std::make_shared<LogicalStatisticFields>(), target.statisticId, target.statisticType);

        const auto roundTripped = unreflect<StatisticStoreWriterLogicalOperator>(NES::reflect(writer));

        EXPECT_EQ(roundTripped.getStatisticId(), target.statisticId);
        EXPECT_EQ(roundTripped.getStatisticType(), target.statisticType);
        EXPECT_EQ(statisticDataFieldName(roundTripped.getStatisticId()), statisticDataFieldName(target.statisticId));
    }
}

}
