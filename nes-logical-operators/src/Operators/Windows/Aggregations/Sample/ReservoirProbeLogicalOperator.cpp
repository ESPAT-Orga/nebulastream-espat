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

#include <Operators/Windows/Aggregations/Sample/ReservoirProbeLogicalOperator.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <Statistic.hpp>

namespace NES
{

ReservoirProbeLogicalOperator::ReservoirProbeLogicalOperator(const Statistic::StatisticId statisticId, const Schema& sampleSchema)
    : statisticId(statisticId), sampleSchema(sampleSchema)
{
}

ReservoirProbeLogicalOperator::ReservoirProbeLogicalOperator(
    Statistic::StatisticId stashHash, const Schema& sampleSchema, LogicalStatisticFields logicalStatisticFields)
    : LogicalStatisticFields(std::move(logicalStatisticFields)), statisticId(stashHash), sampleSchema(sampleSchema)
{
}

std::string_view ReservoirProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool ReservoirProbeLogicalOperator::operator==(const ReservoirProbeLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId and sampleSchema == rhs.sampleSchema and inputSchema == rhs.inputSchema
        and outputSchema == rhs.outputSchema and inputOriginIds == rhs.inputOriginIds and outputOriginIds == rhs.outputOriginIds;
};

ReservoirProbeLogicalOperator ReservoirProbeLogicalOperator::withInferredSchema(const std::vector<Schema>& inputSchemas) const
{
    auto copy = *this;
    INVARIANT(inputSchemas.size() == 1, "ReservoirProbe should have one input schema but got {}", inputSchemas.size());
    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for ReservoirProbe operator");
        }
    }

    /// ReservoirProbeLogicalOperator expects the following fields in its input schema. If not, we need to throw
    copy.inputSchema = firstSchema;
    if (not copy.inputSchema.getFieldByName(copy.statisticStartTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copy.statisticEndTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copy.statisticIdField.name).has_value())
    {
        std::stringstream expectedFields;
        expectedFields << copy.statisticStartTsField << ", " << copy.statisticEndTsField << ", " << copy.statisticIdField;
        throw FieldNotFound("Expected the following fields {} to be in the schema {}.", expectedFields.str(), copy.inputSchema);
    }

    const auto& newQualifierForSystemField = firstSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();
    copy.sampleSchema = Schema();
    for (auto& field : sampleSchema)
    {
        const auto pos = field.name.find(Schema::ATTRIBUTE_NAME_SEPARATOR);
        const auto newFieldName = (pos == std::string::npos) ? newQualifierForSystemField + field.name : field.name;
        copy.sampleSchema.addField(newFieldName, field.dataType);
    }

    copy.outputSchema = Schema();
    copy.statisticIdField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticStartTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticEndTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticNumberOfSeenTuplesField.addQualifierIfNotExists(newQualifierForSystemField);

    copy.outputSchema.addField(copy.statisticIdField);
    copy.outputSchema.addField(copy.statisticStartTsField);
    copy.outputSchema.addField(copy.statisticEndTsField);
    copy.outputSchema.addField(copy.statisticNumberOfSeenTuplesField);
    copy.outputSchema.appendFieldsFromOtherSchema(copy.sampleSchema);

    return copy;
}

ReservoirProbeLogicalOperator ReservoirProbeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet ReservoirProbeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

ReservoirProbeLogicalOperator ReservoirProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> ReservoirProbeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema ReservoirProbeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> ReservoirProbeLogicalOperator::getChildren() const
{
    return children;
}

std::string ReservoirProbeLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("RESERVOIR_PROBE(opId: {}, statHash: {}, sampleSchema: {})", id, statisticId, sampleSchema);
    }
    std::string joined;
    const auto& fields = sampleSchema.getFieldNames();

    for (size_t i = 0; i < fields.size(); ++i)
    {
        if (i > 0)
            joined += ',';
        joined += fields[i];
    }
    return fmt::format("RESERVOIR_PROBE({})", joined);
}

Reflected Reflector<ReservoirProbeLogicalOperator>::operator()(const ReservoirProbeLogicalOperator& op) const
{
    std::vector<detail::ReflectedSchemaField> fields;
    for (const auto& field : op.sampleSchema)
    {
        fields.push_back({field.name, static_cast<uint8_t>(field.dataType.type)});
    }
    return reflect(detail::ReflectedReservoirProbeLogicalOperator{.statisticId = op.statisticId.getRawValue(), .sampleFields = fields});
}

ReservoirProbeLogicalOperator Unreflector<ReservoirProbeLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto [statisticId, sampleFields] = unreflect<detail::ReflectedReservoirProbeLogicalOperator>(reflected);
    Schema sampleSchema;
    for (const auto& field : sampleFields)
    {
        sampleSchema.addField(
            field.name, DataTypeProvider::provideDataType(static_cast<DataType::Type>(field.dataType), DataType::NULLABLE::NOT_NULLABLE));
    }
    return ReservoirProbeLogicalOperator{Statistic::StatisticId{statisticId}, sampleSchema};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterReservoirProbeLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ReservoirProbeLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Expected arguments are missing");
    std::unreachable();
}

}
