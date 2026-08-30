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

#include <ranges>
#include <utility>
#include <fmt/format.h>
#include <LogicalOperatorRegistry.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

StatisticStoreWriterLogicalOperator::StatisticStoreWriterLogicalOperator(
    std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType statisticType)
    : inputLogicalStatisticFields(std::move(inputLogicalStatisticFields)), statisticId(statisticId), statisticType(statisticType)
{
}

std::string StatisticStoreWriterLogicalOperator::explain(ExplainVerbosity, OperatorId id) const
{
    return fmt::format("STATISTIC_STORE_WRITER(opId: {})", id);
}

std::vector<LogicalOperator> StatisticStoreWriterLogicalOperator::getChildren() const
{
    return children;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

bool StatisticStoreWriterLogicalOperator::operator==(const StatisticStoreWriterLogicalOperator& rhs) const
{
    return getChildren() == rhs.getChildren() and getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas()
        and getTraitSet() == rhs.getTraitSet();
}

std::string_view StatisticStoreWriterLogicalOperator::getName() const noexcept
{
    return NAME;
}

TraitSet StatisticStoreWriterLogicalOperator::getTraitSet() const
{
    return traitSet;
}

std::vector<Schema> StatisticStoreWriterLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
}

Schema StatisticStoreWriterLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(inputSchemas.size() == 1, "StatisticStoreWriter should have one input schema but got {}", inputSchemas.size());
    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for StatisticStoreWriter operator");
        }
    }

    /// StatisticStoreWriter expects the following fields in its input schema. If not, we need to throw
    copy.inputSchema = firstSchema;
    const auto& newQualifierForSystemField = firstSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();
    copy.inputLogicalStatisticFields = std::make_shared<LogicalStatisticFields>(*copy.inputLogicalStatisticFields);
    copy.inputLogicalStatisticFields->addQualifierName(newQualifierForSystemField);
    const auto& copyInputLogicalFields = copy.inputLogicalStatisticFields;
    if (not copy.inputSchema.getFieldByName(copyInputLogicalFields->statisticStartTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copyInputLogicalFields->statisticEndTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copyInputLogicalFields->statisticNumberOfSeenTuplesField.name).has_value())
    {
        throw FieldNotFound(
            "Expected statistic metadata fields ({}, {}, {}) in schema {}.",
            copyInputLogicalFields->statisticStartTsField.name,
            copyInputLogicalFields->statisticEndTsField.name,
            copyInputLogicalFields->statisticNumberOfSeenTuplesField.name,
            copy.inputSchema);
    }
    /// The single VARSIZED data field this writer reads, named from its id (statisticDataFieldName), must exist.
    const auto dataFieldName = statisticDataFieldName(copy.statisticId);
    if (not copy.inputSchema.getFieldByName(dataFieldName).has_value())
    {
        throw FieldNotFound("Expected statistic data field {} in schema {}.", dataFieldName, copy.inputSchema);
    }

    /// Pass the full input through (so the next writer in the chain still sees every data field) and add the
    /// STATISTICID field this writer is responsible for. A downstream projection reduces to the sink schema.
    copy.outputSchema = copy.inputSchema;
    const auto outputLogicalStatisticFields = getOutputStatisticFields(newQualifierForSystemField);
    if (not copy.outputSchema.getFieldByName(outputLogicalStatisticFields.statisticIdField.name).has_value())
    {
        copy.outputSchema.addField(outputLogicalStatisticFields.statisticIdField);
    }

    return copy;
}

LogicalStatisticFields StatisticStoreWriterLogicalOperator::getOutputStatisticFields(const std::string_view qualifierName)
{
    return LogicalStatisticFields().addQualifierName(qualifierName);
}

StatisticTuple::StatisticId StatisticStoreWriterLogicalOperator::getStatisticId() const
{
    return statisticId;
}

StatisticTuple::StatisticType StatisticStoreWriterLogicalOperator::getStatisticType() const
{
    return statisticType;
}

Reflected Reflector<StatisticStoreWriterLogicalOperator>::operator()(const StatisticStoreWriterLogicalOperator& op) const
{
    return reflect(detail::ReflectedStatisticStoreWriterLogicalOperator{
        .statisticId = op.getStatisticId().getRawValue(), .statisticType = op.getStatisticType()});
}

StatisticStoreWriterLogicalOperator Unreflector<StatisticStoreWriterLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto [statisticId, statisticType] = unreflect<detail::ReflectedStatisticStoreWriterLogicalOperator>(reflected);
    /// The data field name is re-derived from the id, so a fresh (metadata-only) LogicalStatisticFields is enough.
    return StatisticStoreWriterLogicalOperator{
        std::make_shared<LogicalStatisticFields>(), StatisticTuple::StatisticId{statisticId}, statisticType};
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
