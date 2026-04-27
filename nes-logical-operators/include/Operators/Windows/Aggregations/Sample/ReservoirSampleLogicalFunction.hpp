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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/StatisticLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <Statistic.hpp>

namespace NES
{

/// Builds a reservoir sample via our aggregation functions.
/// Sized by a memory budget; the reservoir size is derived in `calculateConfigs`.
class ReservoirSampleLogicalFunction : public StatisticLogicalFunction
{
public:
    /// `onField` needs to be a valid field, but is otherwise ignored
    /// `asField` used when the reservoir should be renamed in the query
    /// `memoryBudget` budget in bytes used to derive the reservoir size during lowering
    ReservoirSampleLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        std::vector<FieldAccessLogicalFunction> sampleFields,
        uint64_t memoryBudget,
        Statistic::StatisticId statisticId);
    ReservoirSampleLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        const FieldAccessLogicalFunction& asField,
        std::vector<FieldAccessLogicalFunction> sampleFields,
        uint64_t memoryBudget,
        Statistic::StatisticId statisticId);

    ~ReservoirSampleLogicalFunction() override = default;

    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] ReservoirSampleLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] ReservoirSampleLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] ReservoirSampleLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] ReservoirSampleLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] ReservoirSampleLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] ReservoirSampleLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;

    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const ReservoirSampleLogicalFunction& rhs) const;

    [[nodiscard]] std::unique_ptr<StatisticConfig> calculateConfigs() const override;

    /// Selects which fields get projected into the sample.
    std::vector<FieldAccessLogicalFunction> sampleFields;
    /// Identifies the sample in the StatStore
    Statistic::StatisticId statisticId;

private:
    static constexpr std::string_view NAME = "ReservoirSample";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<ReservoirSampleLogicalFunction>);

template <>
struct Reflector<ReservoirSampleLogicalFunction>
{
    Reflected operator()(const ReservoirSampleLogicalFunction& function) const;
};

template <>
struct Unreflector<ReservoirSampleLogicalFunction>
{
    ReservoirSampleLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedReservoirSampleLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    std::vector<FieldAccessLogicalFunction> sampleFields;
    uint64_t memoryBudget;
    Statistic::StatisticId::Underlying statisticId;
};
}
