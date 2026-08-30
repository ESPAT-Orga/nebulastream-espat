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

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <StatisticTuple.hpp>

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
        AggregationFieldAccess inputFunction,
        StatisticTuple::StatisticId statisticId,
        StatisticTuple::StatisticType op);

    ScalarStatisticLogicalFunction(
        AggregationFieldAccess inputFunction,
        DataType aggregateType,
        StatisticTuple::StatisticId statisticId,
        StatisticTuple::StatisticType op);

    ~ScalarStatisticLogicalFunction() = default;

    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;

    [[nodiscard]] ScalarStatisticLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const ScalarStatisticLogicalFunction& rhs) const;

    StatisticTuple::StatisticId statisticId;
    StatisticTuple::StatisticType op;

private:
    static constexpr std::string_view NAME = "ScalarStatistic";

    AggregationFieldAccess inputFunction;
    DataType aggregateType;
};

static_assert(WindowAggregationFunctionConcept<ScalarStatisticLogicalFunction>);

template <>
struct Reflector<ScalarStatisticLogicalFunction>
{
    Reflected operator()(const ScalarStatisticLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ScalarStatisticLogicalFunction>
{
    ScalarStatisticLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

namespace NES::detail
{
struct ReflectedScalarStatisticLogicalFunction
{
    StatisticTuple::StatisticId::Underlying statisticId;
    uint64_t op;
    AggregationFieldAccess inputFunction;
};
}

namespace std
{
template <>
struct hash<NES::ScalarStatisticLogicalFunction>
{
    size_t operator()(const NES::ScalarStatisticLogicalFunction& function) const noexcept;
};
}
