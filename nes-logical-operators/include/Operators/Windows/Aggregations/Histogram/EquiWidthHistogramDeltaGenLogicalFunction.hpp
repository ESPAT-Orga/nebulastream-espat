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

/// GEN side of histogram delta compression. Identical to EquiWidthHistogramLogicalFunction except its
/// name ("EquiWidthHistogramDeltaGen") selects the delta-gen physical function during lowering. Build is
/// the standard histogram build; the difference is at lower (emit sparse delta against the baseline).
class EquiWidthHistogramDeltaGenLogicalFunction : public StatisticLogicalFunction
{
public:
    EquiWidthHistogramDeltaGenLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        uint64_t memoryBudget,
        uint64_t minValue,
        uint64_t maxValue,
        Statistic::StatisticId statisticId);
    EquiWidthHistogramDeltaGenLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        const FieldAccessLogicalFunction& asField,
        uint64_t memoryBudget,
        uint64_t minValue,
        uint64_t maxValue,
        Statistic::StatisticId statisticId);

    ~EquiWidthHistogramDeltaGenLogicalFunction() override = default;

    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] EquiWidthHistogramDeltaGenLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] EquiWidthHistogramDeltaGenLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] EquiWidthHistogramDeltaGenLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] EquiWidthHistogramDeltaGenLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] EquiWidthHistogramDeltaGenLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] EquiWidthHistogramDeltaGenLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;

    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const EquiWidthHistogramDeltaGenLogicalFunction& rhs) const;

    [[nodiscard]] std::unique_ptr<StatisticConfig> calculateConfigs() const override;

    uint64_t minValue;
    uint64_t maxValue;

    Statistic::StatisticId statisticId;

private:
    static constexpr std::string_view NAME = "EquiWidthHistogramDeltaGen";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<EquiWidthHistogramDeltaGenLogicalFunction>);

template <>
struct Reflector<EquiWidthHistogramDeltaGenLogicalFunction>
{
    Reflected operator()(const EquiWidthHistogramDeltaGenLogicalFunction& function) const;
};

template <>
struct Unreflector<EquiWidthHistogramDeltaGenLogicalFunction>
{
    EquiWidthHistogramDeltaGenLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedEquiWidthHistogramDeltaGenLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    uint64_t memoryBudget;
    uint64_t minValue;
    uint64_t maxValue;
    Statistic::StatisticId::Underlying statisticId;
};
}
