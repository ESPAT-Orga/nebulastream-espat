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

#include <Operators/Windows/Aggregations/Scalar/ScalarStatisticProbeLogicalOperator.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

ScalarStatisticProbeLogicalOperator::ScalarStatisticProbeLogicalOperator(
    WeakLogicalOperator self,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType op,
    DataType valueType)
    : ManagedByOperator(std::move(self))
    , statisticId(statisticId)
    , op(op)
    , valueType(std::move(valueType))
{
}

ScalarStatisticProbeLogicalOperator::ScalarStatisticProbeLogicalOperator(
    WeakLogicalOperator self,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType op,
    DataType valueType,
    std::string valueFieldName,
    LogicalStatisticFields logicalStatisticFields)
    : LogicalStatisticFields(std::move(logicalStatisticFields))
    , ManagedByOperator(std::move(self))
    , statisticId(statisticId)
    , op(op)
    , valueType(std::move(valueType))
    , valueFieldName(std::move(valueFieldName))
{
}

ScalarStatisticProbeLogicalOperator::ScalarStatisticProbeLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType op,
    DataType valueType)
    : ManagedByOperator(std::move(self))
    , statisticId(statisticId)
    , op(op)
    , valueType(std::move(valueType))
    , child(std::move(child))
{
    inferLocalSchema();
}

TypedLogicalOperator<ScalarStatisticProbeLogicalOperator> ScalarStatisticProbeLogicalOperator::create(
    StatisticTuple::StatisticId statisticId,
    StatisticTuple::StatisticType op,
    DataType valueType)
{
    return TypedLogicalOperator<ScalarStatisticProbeLogicalOperator>{statisticId, op, std::move(valueType)};
}

void ScalarStatisticProbeLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const Schema<Field, Unordered>& inputSchema = child->getOutputSchema();

    const auto startFieldName = Identifier::parse(statisticStartTsField.name);
    const auto endFieldName = Identifier::parse(statisticEndTsField.name);
    const auto idFieldName = Identifier::parse(statisticIdField.name);

    if (!inputSchema[startFieldName].has_value() || !inputSchema[endFieldName].has_value()
        || !inputSchema[idFieldName].has_value())
    {
        throw FieldNotFound(
            "Expected statistic metadata fields ({}, {}, {}) in input schema.",
            statisticStartTsField.name,
            statisticEndTsField.name,
            statisticIdField.name);
    }

    std::vector<UnqualifiedUnboundField> outputFields;
    outputFields.emplace_back(idFieldName, statisticIdField.dataType);
    outputFields.emplace_back(startFieldName, statisticStartTsField.dataType);
    outputFields.emplace_back(endFieldName, statisticEndTsField.dataType);
    outputFields.emplace_back(Identifier::parse(statisticNumberOfSeenTuplesField.name), statisticNumberOfSeenTuplesField.dataType);
    outputFields.emplace_back(Identifier::parse(valueFieldName), valueType);

    const auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in scalar statistic probe output schema: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    outputSchema = outputSchemaOrCollisions.value();
}

std::string_view ScalarStatisticProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool ScalarStatisticProbeLogicalOperator::operator==(const ScalarStatisticProbeLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId && op == rhs.op && valueType == rhs.valueType && outputSchema == rhs.outputSchema
        && traitSet == rhs.traitSet && child == rhs.child;
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet ScalarStatisticProbeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for ScalarStatisticProbe, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for ScalarStatisticProbe, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

Schema<Field, Unordered> ScalarStatisticProbeLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Retrieving output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> ScalarStatisticProbeLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

std::string ScalarStatisticProbeLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SCALARSTATISTIC_PROBE(opId: {}, statId: {}, op: {}, valueType: {})", id, statisticId, magic_enum::enum_name(op), valueType);
    }
    return fmt::format("SCALARSTATISTIC_PROBE()");
}

Reflected Reflector<ScalarStatisticProbeLogicalOperator>::operator()(
    const ScalarStatisticProbeLogicalOperator& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedScalarStatisticProbeLogicalOperator{
        .statisticId = op.statisticId.getRawValue(),
        .op = static_cast<uint64_t>(op.op),
        .valueType = op.valueType,
        .valueFieldName = op.valueFieldName});
}

ScalarStatisticProbeLogicalOperator Unreflector<ScalarStatisticProbeLogicalOperator>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto data = context.unreflect<detail::ReflectedScalarStatisticProbeLogicalOperator>(reflected);
    return ScalarStatisticProbeLogicalOperator{
        WeakLogicalOperator{},
        StatisticTuple::StatisticId{data.statisticId},
        static_cast<StatisticTuple::StatisticType>(data.op),
        data.valueType,
        std::move(data.valueFieldName),
        LogicalStatisticFields{}};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterScalarStatisticProbeLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ScalarStatisticProbeLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Expected arguments are missing");
    std::unreachable();
}

}
