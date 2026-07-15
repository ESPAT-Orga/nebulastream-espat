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
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <Statistic.hpp>

namespace NES
{

/// A store-backed scalar statistic (Count / Sum / Avg). Unlike the synopsis statistics
/// (CountMinSketch / EquiWidthHistogram / ReservoirSample) it carries no memory budget: its persisted
/// payload is the bare aggregate. The three ops share ONE plugin name ("ScalarStatistic") and are told
/// apart by the op, which selects the StatisticType stored, the SQL surface
/// (SUMSTATISTIC / COUNTSTATISTIC / AVGSTATISTIC), the stamps inferred below, and which physical
/// function the registrar builds (see ScalarStatisticPhysicalFunction.hpp). Used to benchmark the
/// StatisticStoreWriter overhead for a minimal synopsis payload.
class ScalarStatisticLogicalFunction
{
public:
    ScalarStatisticLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        const FieldAccessLogicalFunction& asField,
        Statistic::StatisticId statisticId,
        Statistic::StatisticType op);
    ~ScalarStatisticLogicalFunction() = default;

    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] ScalarStatisticLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] ScalarStatisticLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] ScalarStatisticLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] ScalarStatisticLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] ScalarStatisticLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] ScalarStatisticLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;

    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const ScalarStatisticLogicalFunction& rhs) const;

    Statistic::StatisticId statisticId;
    Statistic::StatisticType op;

private:
    static constexpr std::string_view NAME = "ScalarStatistic";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<ScalarStatisticLogicalFunction>);

template <>
struct Reflector<ScalarStatisticLogicalFunction>
{
    Reflected operator()(const ScalarStatisticLogicalFunction& function) const;
};

template <>
struct Unreflector<ScalarStatisticLogicalFunction>
{
    ScalarStatisticLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedScalarStatisticLogicalFunction
{
    Statistic::StatisticId::Underlying statisticId;
    uint64_t op;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};
}
