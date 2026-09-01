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
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// A store-backed scalar statistic (Count / Sum / Avg).
///
/// Unlike the synopsis statistics on the branch this is ported from (equi-width histogram, count-min sketch,
/// reservoir sample) it carries no memory budget: its persisted payload is the bare aggregate. All three ops
/// share ONE plugin name and are told apart by 'op', which selects the StatisticType written to the store and
/// which physical function the registry builds.
///
/// The aggregate type is VARSIZED rather than the scalar's own type on purpose. This is the whole reason a
/// plain Avg cannot be used here: StatisticStoreWriter reads its payload field with
/// getRawValueAs<VariableSizedData>(), which throws on any other VarVal variant, so the aggregation that feeds
/// it has to hand over variable-sized data. The physical function wraps the scalar accordingly.
///
/// The result field is named by the caller through ProjectedAggregation::name -- pass
/// statisticDataFieldName(statisticId) so the writer finds the payload under the name it derives from the id.
///
/// No 'create' member and no add_registry_entry: name-based construction exists for the SQL parser, which this
/// port deliberately does not have, and AggregationLogicalFunctionRegistryArguments carries neither the
/// statisticId nor the op, so it could not reconstruct one anyway. Unreflection is registered, since that is
/// what plan (de)serialization needs.
class ScalarStatisticAggregationLogicalFunction final
{
public:
    ScalarStatisticAggregationLogicalFunction(AggregationFieldAccess inputFunction, StatisticId statisticId, StatisticType op);

    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] StatisticId getStatisticId() const;
    [[nodiscard]] StatisticType getOp() const;
    [[nodiscard]] ScalarStatisticAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const ScalarStatisticAggregationLogicalFunction& other) const;

    /// The persisted payload is a bare scalar with no null bit, and StatisticStoreWriter requires every field
    /// it reads to be NOT_NULLABLE, so nulls are excluded rather than aggregated over.
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

private:
    AggregationFieldAccess inputFunction;
    StatisticId statisticId;
    StatisticType op;
    static constexpr std::string_view NAME = "ScalarStatistic";
};

template <>
struct Reflector<ScalarStatisticAggregationLogicalFunction>
{
    Reflected operator()(const ScalarStatisticAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ScalarStatisticAggregationLogicalFunction>
{
    ScalarStatisticAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

namespace detail
{
/// Strong types are flattened to their underlying representation so the reflection machinery needs no
/// Reflector specialisation for StatisticId or StatisticType.
struct ReflectedScalarStatisticAggregationLogicalFunction
{
    AggregationFieldAccess inputFunction;
    uint64_t statisticId;
    uint64_t op;
};
}

}

template <>
struct std::hash<NES::ScalarStatisticAggregationLogicalFunction>
{
    size_t operator()(const NES::ScalarStatisticAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::ScalarStatisticAggregationLogicalFunction>);
