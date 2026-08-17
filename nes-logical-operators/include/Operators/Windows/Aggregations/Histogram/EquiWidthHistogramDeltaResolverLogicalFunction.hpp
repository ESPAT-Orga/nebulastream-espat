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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/StatisticLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <Statistic.hpp>

namespace NES
{

/// RESOLVER side of histogram delta compression. Its name ("EquiWidthHistogramDeltaResolver") selects the
/// delta-resolver physical function during lowering: lift applies the incoming delta blob onto the zeroed
/// state, and lower adds the previous resolved window (baseline) and emits the full histogram.
/// `onField` is the incoming delta blob field (not a raw data field).
class EquiWidthHistogramDeltaResolverLogicalFunction : public StatisticLogicalFunction
{
public:
    EquiWidthHistogramDeltaResolverLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        uint64_t memoryBudget,
        uint64_t minValue,
        uint64_t maxValue,
        Statistic::StatisticId statisticId);
    EquiWidthHistogramDeltaResolverLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        const FieldAccessLogicalFunction& asField,
        uint64_t memoryBudget,
        uint64_t minValue,
        uint64_t maxValue,
        Statistic::StatisticId statisticId);

    ~EquiWidthHistogramDeltaResolverLogicalFunction() override = default;

    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] EquiWidthHistogramDeltaResolverLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] EquiWidthHistogramDeltaResolverLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] EquiWidthHistogramDeltaResolverLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] EquiWidthHistogramDeltaResolverLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] EquiWidthHistogramDeltaResolverLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] EquiWidthHistogramDeltaResolverLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;

    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const EquiWidthHistogramDeltaResolverLogicalFunction& rhs) const;

    [[nodiscard]] std::unique_ptr<StatisticConfig> calculateConfigs() const override;

    uint64_t minValue;
    uint64_t maxValue;

    Statistic::StatisticId statisticId;

private:
    static constexpr std::string_view NAME = "EquiWidthHistogramDeltaResolver";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<EquiWidthHistogramDeltaResolverLogicalFunction>);

template <>
struct Reflector<EquiWidthHistogramDeltaResolverLogicalFunction>
{
    Reflected operator()(const EquiWidthHistogramDeltaResolverLogicalFunction& function) const;
};

template <>
struct Unreflector<EquiWidthHistogramDeltaResolverLogicalFunction>
{
    EquiWidthHistogramDeltaResolverLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedEquiWidthHistogramDeltaResolverLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    uint64_t memoryBudget;
    uint64_t minValue;
    uint64_t maxValue;
    Statistic::StatisticId::Underlying statisticId;
};
}
