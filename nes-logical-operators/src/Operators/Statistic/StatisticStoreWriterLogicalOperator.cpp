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

#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>

#include <optional>
#include <ranges>
#include <utility>
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

StatisticStoreWriterLogicalOperator::StatisticStoreWriterLogicalOperator(
    WeakLogicalOperator self,
    std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType statisticType)
    : ManagedByOperator(std::move(self))
    , inputLogicalStatisticFields(std::move(inputLogicalStatisticFields))
    , statisticId(statisticId)
    , statisticType(statisticType)
{
}

StatisticStoreWriterLogicalOperator::StatisticStoreWriterLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType statisticType)
    : ManagedByOperator(std::move(self))
    , inputLogicalStatisticFields(std::move(inputLogicalStatisticFields))
    , statisticId(statisticId)
    , statisticType(statisticType)
    , child(std::move(child))
{
    inferLocalSchema();
}

TypedLogicalOperator<StatisticStoreWriterLogicalOperator> StatisticStoreWriterLogicalOperator::create(
    std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
    StatisticTuple::StatisticId statisticId,
    StatisticTuple::StatisticType statisticType)
{
    return TypedLogicalOperator<StatisticStoreWriterLogicalOperator>{
        std::move(inputLogicalStatisticFields), statisticId, statisticType};
}

void StatisticStoreWriterLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const Schema<Field, Unordered>& inputSchema = child->getOutputSchema();

    const auto startFieldName = Identifier::parse(inputLogicalStatisticFields->statisticStartTsField.name);
    const auto endFieldName = Identifier::parse(inputLogicalStatisticFields->statisticEndTsField.name);
    const auto seenTuplesFieldName = Identifier::parse(inputLogicalStatisticFields->statisticNumberOfSeenTuplesField.name);
    const auto dataFieldName = Identifier::parse(statisticDataFieldName(statisticId));

    if (!inputSchema[startFieldName].has_value() || !inputSchema[endFieldName].has_value()
        || !inputSchema[seenTuplesFieldName].has_value())
    {
        throw FieldNotFound(
            "Expected statistic metadata fields ({}, {}, {}) in input schema.",
            inputLogicalStatisticFields->statisticStartTsField.name,
            inputLogicalStatisticFields->statisticEndTsField.name,
            inputLogicalStatisticFields->statisticNumberOfSeenTuplesField.name);
    }
    if (!inputSchema[dataFieldName].has_value())
    {
        throw FieldNotFound(
            "Expected statistic data field {} in input schema.", statisticDataFieldName(statisticId));
    }

    /// Pass the full input through (so the next writer in the chain still sees every data field) and add the
    /// STATISTICID field this writer is responsible for. A downstream projection reduces to the sink schema.
    std::vector<UnqualifiedUnboundField> outputFields;
    for (const auto& field : inputSchema)
    {
        outputFields.emplace_back(field.unbound());
    }
    const auto statisticIdFieldName = Identifier::parse(inputLogicalStatisticFields->statisticIdField.name);
    if (!inputSchema[statisticIdFieldName].has_value())
    {
        outputFields.emplace_back(statisticIdFieldName, inputLogicalStatisticFields->statisticIdField.dataType);
    }

    const auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in statistic store writer output schema: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    outputSchema = outputSchemaOrCollisions.value();
}

std::string StatisticStoreWriterLogicalOperator::explain(ExplainVerbosity, OperatorId id) const
{
    return fmt::format("STATISTIC_STORE_WRITER(opId: {})", id);
}

std::vector<LogicalOperator> StatisticStoreWriterLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticStoreWriter, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticStoreWriter, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

bool StatisticStoreWriterLogicalOperator::operator==(const StatisticStoreWriterLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId && statisticType == rhs.statisticType && outputSchema == rhs.outputSchema
        && traitSet == rhs.traitSet && child == rhs.child;
}

std::string_view StatisticStoreWriterLogicalOperator::getName() const noexcept
{
    return NAME;
}

TraitSet StatisticStoreWriterLogicalOperator::getTraitSet() const
{
    return traitSet;
}

Schema<Field, Unordered> StatisticStoreWriterLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Retrieving output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

StatisticTuple::StatisticId StatisticStoreWriterLogicalOperator::getStatisticId() const
{
    return statisticId;
}

StatisticTuple::StatisticType StatisticStoreWriterLogicalOperator::getStatisticType() const
{
    return statisticType;
}

LogicalStatisticFields StatisticStoreWriterLogicalOperator::getOutputStatisticFields(const std::string_view qualifierName)
{
    return LogicalStatisticFields().addQualifierName(qualifierName);
}

Reflected Reflector<StatisticStoreWriterLogicalOperator>::operator()(
    const StatisticStoreWriterLogicalOperator& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedStatisticStoreWriterLogicalOperator{
        .statisticId = op.getStatisticId().getRawValue(), .statisticType = op.getStatisticType()});
}

StatisticStoreWriterLogicalOperator Unreflector<StatisticStoreWriterLogicalOperator>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [statisticId, statisticType] = context.unreflect<detail::ReflectedStatisticStoreWriterLogicalOperator>(reflected);
    /// The data field name is re-derived from the id, so a fresh (metadata-only) LogicalStatisticFields is enough.
    return StatisticStoreWriterLogicalOperator{
        WeakLogicalOperator{}, std::make_shared<LogicalStatisticFields>(), StatisticTuple::StatisticId{statisticId}, statisticType};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterStatisticStoreWriterLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<StatisticStoreWriterLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Expected arguments are missing");
    std::unreachable();
}

}
