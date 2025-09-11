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
#include <Serialization/SchemaSerializationUtil.hpp>
#include <fmt/format.h>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

StatisticStoreWriterLogicalOperator::StatisticStoreWriterLogicalOperator(
    std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
    const Statistic::StatisticHash statisticHash,
    const Statistic::StatisticType statisticType)
    : inputLogicalStatisticFields(std::move(inputLogicalStatisticFields)), statisticHash(statisticHash), statisticType(statisticType)
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

void StatisticStoreWriterLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
    const auto inputs = getInputSchemas();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    auto serializeAndAddToConfig = [&serializableOperator](const Schema::Field& field, const std::string& configKey)
    {
        SerializableSchema_SerializableField inputSerializableField;
        SchemaSerializationUtil::serializeField(field, &inputSerializableField);
        (*serializableOperator.mutable_config())[configKey] = descriptorConfigTypeToProto(inputSerializableField);
    };
    serializeAndAddToConfig(inputLogicalStatisticFields->statisticEndTsField, LogicalStatisticFields::ConfigParameters::STATISTIC_END_TS);
    serializeAndAddToConfig(
        inputLogicalStatisticFields->statisticStartTsField, LogicalStatisticFields::ConfigParameters::STATISTIC_START_TS);
    serializeAndAddToConfig(
        inputLogicalStatisticFields->statisticHashField, LogicalStatisticFields::ConfigParameters::STATISTIC_HASH_FIELD);
    serializeAndAddToConfig(inputLogicalStatisticFields->statisticDataField, LogicalStatisticFields::ConfigParameters::STATISTIC_DATA);
    serializeAndAddToConfig(
        inputLogicalStatisticFields->statisticNumberOfSeenTuplesField,
        LogicalStatisticFields::ConfigParameters::STATISTIC_NUMBER_OF_SEEN_TUPLES);
    (*serializableOperator.mutable_config())[StatisticStoreWriterLogicalOperator::ConfigParameters::STATISTIC_HASH_VALUE]
        = descriptorConfigTypeToProto(statisticHash);
    (*serializableOperator.mutable_config())[StatisticStoreWriterLogicalOperator::ConfigParameters::STATISTIC_TYPE_VALUE]
        = descriptorConfigTypeToProto(EnumWrapper{statisticType});

    serializableOperator.mutable_operator_()->CopyFrom(proto);
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
    const auto& copyInputLogicalFields = copy.inputLogicalStatisticFields;
    if (not copy.inputSchema.getFieldByName(copyInputLogicalFields->statisticStartTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copyInputLogicalFields->statisticEndTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copyInputLogicalFields->statisticDataField.name).has_value()
        or not copy.inputSchema.getFieldByName(copyInputLogicalFields->statisticNumberOfSeenTuplesField.name).has_value())
    {
        std::stringstream expectedFields;
        expectedFields << copyInputLogicalFields->statisticStartTsField << ", " << copyInputLogicalFields->statisticEndTsField << ", "
                       << copyInputLogicalFields->statisticDataField << ", " << copyInputLogicalFields->statisticNumberOfSeenTuplesField;
        throw FieldNotFound("Expected the following fields {} to be in the schema {}.", expectedFields.str(), copy.inputSchema);
    }

    /// We set the output logical fields to use the values defined in the class itself, as downstream operators assume it.
    copy.outputSchema = Schema{firstSchema.memoryLayoutType};
    const auto& newQualifierForSystemField = firstSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();
    const auto outputLogicalStatisticFields = getOutputStatisticFields(newQualifierForSystemField);
    copy.outputSchema.addField(outputLogicalStatisticFields.statisticHashField);
    copy.outputSchema.addField(outputLogicalStatisticFields.statisticStartTsField);
    copy.outputSchema.addField(outputLogicalStatisticFields.statisticEndTsField);
    copy.outputSchema.addField(outputLogicalStatisticFields.statisticNumberOfSeenTuplesField);

    return copy;
}

LogicalStatisticFields StatisticStoreWriterLogicalOperator::getOutputStatisticFields(const std::string_view qualifierName)
{
    return LogicalStatisticFields().addQualifierName(qualifierName);
}

Statistic::StatisticHash StatisticStoreWriterLogicalOperator::getStatisticHash() const
{
    return statisticHash;
}

Statistic::StatisticType StatisticStoreWriterLogicalOperator::getStatisticType() const
{
    return statisticType;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterStatisticStoreWriterLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto statisticEndTs = arguments.config[LogicalStatisticFields::ConfigParameters::STATISTIC_END_TS];
    auto statisticStartTs = arguments.config[LogicalStatisticFields::ConfigParameters::STATISTIC_START_TS];
    auto statisticHashField = arguments.config[LogicalStatisticFields::ConfigParameters::STATISTIC_HASH_FIELD];
    auto statisticDataField = arguments.config[LogicalStatisticFields::ConfigParameters::STATISTIC_DATA];
    auto statisticNumberOfSeenTuples = arguments.config[LogicalStatisticFields::ConfigParameters::STATISTIC_NUMBER_OF_SEEN_TUPLES];
    auto statisticHashValue = arguments.config[StatisticStoreWriterLogicalOperator::ConfigParameters::STATISTIC_HASH_VALUE];
    auto statisticTypeEnumWrapper = arguments.config[StatisticStoreWriterLogicalOperator::ConfigParameters::STATISTIC_TYPE_VALUE];
    if (not std::holds_alternative<NES::SerializableSchema_SerializableField>(statisticEndTs)
        or not std::holds_alternative<NES::SerializableSchema_SerializableField>(statisticStartTs)
        or not std::holds_alternative<NES::SerializableSchema_SerializableField>(statisticHashField)
        or not std::holds_alternative<NES::SerializableSchema_SerializableField>(statisticDataField)
        or not std::holds_alternative<NES::SerializableSchema_SerializableField>(statisticNumberOfSeenTuples)
        or not std::holds_alternative<Statistic::StatisticHash>(statisticHashValue)
        or not std::holds_alternative<EnumWrapper>(statisticTypeEnumWrapper))
    {
        throw UnknownLogicalOperator();
    }

    auto logicalStatisticFields = std::make_shared<LogicalStatisticFields>();
    logicalStatisticFields->statisticEndTsField
        = SchemaSerializationUtil::deserializeField(std::get<NES::SerializableSchema_SerializableField>(statisticEndTs));
    logicalStatisticFields->statisticStartTsField
        = SchemaSerializationUtil::deserializeField(std::get<NES::SerializableSchema_SerializableField>(statisticStartTs));
    logicalStatisticFields->statisticHashField
        = SchemaSerializationUtil::deserializeField(std::get<NES::SerializableSchema_SerializableField>(statisticHashField));
    logicalStatisticFields->statisticDataField
        = SchemaSerializationUtil::deserializeField(std::get<NES::SerializableSchema_SerializableField>(statisticDataField));
    logicalStatisticFields->statisticNumberOfSeenTuplesField
        = SchemaSerializationUtil::deserializeField(std::get<NES::SerializableSchema_SerializableField>(statisticNumberOfSeenTuples));

    auto statisticTypeValue = std::get<EnumWrapper>(statisticTypeEnumWrapper).asEnum<Statistic::StatisticType>();
    if (not statisticTypeValue.has_value())
    {
        throw UnknownLogicalOperator();
    }
    const StatisticStoreWriterLogicalOperator logicalOperator(
        std::move(logicalStatisticFields), std::get<Statistic::StatisticHash>(statisticHashValue), statisticTypeValue.value());
    return logicalOperator.withInferredSchema(arguments.inputSchemas);
}
}
