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
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

/// Builds a new stream by reading back the single value of a scalar statistic (Count / Sum / Avg). One operator serves
/// all three ops: their payload is the same bare scalar and they differ only in its data type, which the probe must be
/// told because nothing links it back to the build's on-field (see the SQL surface in AntlrSQLQueryPlanCreator).
/// The statisticType is carried because StatisticProvider dispatches its iterator on it.
class ScalarStatisticProbeLogicalOperator final : public LogicalStatisticFields, public OriginIdAssigner, public ManagedByOperator
{
public:
    explicit ScalarStatisticProbeLogicalOperator(
        WeakLogicalOperator self,
        StatisticTuple::StatisticId statisticId,
        StatisticTuple::StatisticType op,
        DataType valueType);

    explicit ScalarStatisticProbeLogicalOperator(
        WeakLogicalOperator self,
        StatisticTuple::StatisticId statisticId,
        StatisticTuple::StatisticType op,
        DataType valueType,
        std::string valueFieldName,
        LogicalStatisticFields logicalStatisticFields);

    explicit ScalarStatisticProbeLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        StatisticTuple::StatisticId statisticId,
        StatisticTuple::StatisticType op,
        DataType valueType);

    static TypedLogicalOperator<ScalarStatisticProbeLogicalOperator> create(
        StatisticTuple::StatisticId statisticId,
        StatisticTuple::StatisticType op,
        DataType valueType);

    [[nodiscard]] bool operator==(const ScalarStatisticProbeLogicalOperator& rhs) const;

    [[nodiscard]] ScalarStatisticProbeLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] ScalarStatisticProbeLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] ScalarStatisticProbeLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] ScalarStatisticProbeLogicalOperator withInferredSchema() const;

    StatisticTuple::StatisticId statisticId;
    StatisticTuple::StatisticType op;
    DataType valueType;

    std::string valueFieldName = VALUE_FIELD_NAME_DEFAULT;

private:
    static constexpr std::string_view NAME = "ScalarStatisticProbe";

    static constexpr std::string VALUE_FIELD_NAME_DEFAULT = "STATISTICVALUE";

    std::optional<LogicalOperator> child;
    TraitSet traitSet;

    void inferLocalSchema();
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

template <>
struct Reflector<ScalarStatisticProbeLogicalOperator>
{
    Reflected operator()(const ScalarStatisticProbeLogicalOperator& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ScalarStatisticProbeLogicalOperator>
{
    ScalarStatisticProbeLogicalOperator operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<ScalarStatisticProbeLogicalOperator>);

namespace detail
{
struct ReflectedScalarStatisticProbeLogicalOperator
{
    StatisticTuple::StatisticId::Underlying statisticId;
    uint64_t op;
    DataType valueType;
    std::string valueFieldName;
};
}

}
