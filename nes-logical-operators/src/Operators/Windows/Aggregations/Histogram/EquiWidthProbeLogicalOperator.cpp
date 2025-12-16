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

#include <Operators/Windows/Aggregations/Histogram/EquiWidthProbeLogicalOperator.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>
#include <Serialization/DataTypeSerializationUtil.hpp>

namespace NES
{

EquiWidthProbeLogicalOperator::EquiWidthProbeLogicalOperator(const uint64_t statisticHash, DataType counterType, DataType startEndType)
    : statisticHash(statisticHash), counterType(counterType), startEndType(startEndType)
{
}

EquiWidthProbeLogicalOperator::EquiWidthProbeLogicalOperator(
    uint64_t stashHash, DataType counterType, DataType startEndType, LogicalStatisticFields logicalStatisticFields)
    : LogicalStatisticFields(std::move(logicalStatisticFields)), statisticHash(stashHash), counterType(counterType), startEndType(startEndType)
{
}

std::string_view EquiWidthProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool EquiWidthProbeLogicalOperator::operator==(const EquiWidthProbeLogicalOperator& rhs) const
{
    return statisticHash == rhs.statisticHash and counterType == rhs.counterType and startEndType == rhs.startEndType and inputSchema == rhs.inputSchema and outputSchema == rhs.outputSchema and inputOriginIds == rhs.inputOriginIds and outputOriginIds == rhs.outputOriginIds;
};

EquiWidthProbeLogicalOperator EquiWidthProbeLogicalOperator::withInferredSchema(const std::vector<Schema>& inputSchemas) const
{
    auto copy = *this;
    INVARIANT(inputSchemas.size() == 1, "EquiWidthProbe should have one input schema but got {}", inputSchemas.size());
    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for EquiWidthProbe operator");
        }
    }

    /// EquiWidthProbeLogicalOperator expects the following fields in its input schema. If not, we need to throw
    copy.inputSchema = firstSchema;
    if (not copy.inputSchema.getFieldByName(copy.statisticStartTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copy.statisticEndTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copy.statisticHashField.name).has_value())
    {
        std::stringstream expectedFields;
        expectedFields << copy.statisticStartTsField << ", " << copy.statisticEndTsField << ", " << copy.statisticHashField;
        throw FieldNotFound("Expected the following fields {} to be in the schema {}.", expectedFields.str(), copy.inputSchema);
    }

    const auto& newQualifierForSystemField = firstSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();

    copy.outputSchema = Schema{firstSchema.memoryLayoutType};
    copy.statisticHashField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticStartTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticEndTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticNumberOfSeenTuplesField.addQualifierIfNotExists(newQualifierForSystemField);

    copy.outputSchema.addField(copy.statisticHashField);
    copy.outputSchema.addField(copy.statisticStartTsField);
    copy.outputSchema.addField(copy.statisticEndTsField);
    copy.outputSchema.addField(copy.statisticNumberOfSeenTuplesField);

    auto key = Schema::Field("BUCKETKEY", DataType{DataType::Type::UINT64});
    key.addQualifierIfNotExists(newQualifierForSystemField);
    copy.outputSchema.addField(key);
    auto value = Schema::Field("BUCKETVALUE", DataType{DataType::Type::UINT64});
    value.addQualifierIfNotExists(newQualifierForSystemField);
    copy.outputSchema.addField(value);

    return copy;
}

EquiWidthProbeLogicalOperator EquiWidthProbeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet EquiWidthProbeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

EquiWidthProbeLogicalOperator EquiWidthProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> EquiWidthProbeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema EquiWidthProbeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> EquiWidthProbeLogicalOperator::getChildren() const
{
    return children;
}

std::string EquiWidthProbeLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("EQUIWIDTH_PROBE(opId: {}, statHash: {}, counterType: {}, startEndType: {})", id, statisticHash, counterType, startEndType);
    }
    return fmt::format("EQUIWIDTH_PROBE()", statisticHash);
}

void EquiWidthProbeLogicalOperator::serialize(SerializableOperator& serializableOperator) const
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

    serializeAndAddToConfig(statisticEndTsField, LogicalStatisticFields::ConfigParameters::STATISTIC_END_TS);
    serializeAndAddToConfig(statisticStartTsField, LogicalStatisticFields::ConfigParameters::STATISTIC_START_TS);
    serializeAndAddToConfig(statisticHashField, LogicalStatisticFields::ConfigParameters::STATISTIC_HASH_FIELD);
    serializeAndAddToConfig(statisticNumberOfSeenTuplesField, LogicalStatisticFields::ConfigParameters::STATISTIC_NUMBER_OF_SEEN_TUPLES);
    (*serializableOperator.mutable_config())[ConfigParameters::STATISTIC_HASH] = descriptorConfigTypeToProto(statisticHash);
    SerializableDataType counterTypeSerializable;
    (*serializableOperator.mutable_config())[ConfigParameters::COUNTER_TYPE] = descriptorConfigTypeToProto(std::string(magic_enum::enum_name<DataType::Type>(counterType.type)));
    (*serializableOperator.mutable_config())[ConfigParameters::START_END_TYPE] = descriptorConfigTypeToProto(std::string(magic_enum::enum_name<DataType::Type>(startEndType.type)));;

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterEquiWidthProbeLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto statisticHashValue = std::get_if<uint64_t>(&arguments.config[EquiWidthProbeLogicalOperator::ConfigParameters::STATISTIC_HASH]);
    INVARIANT(statisticHashValue, "Wrong value in ConfigParameter::STATISTIC_HASH!");

    auto counterTypeValue = std::get_if<std::string>(&arguments.config[EquiWidthProbeLogicalOperator::ConfigParameters::COUNTER_TYPE]);
    INVARIANT(counterTypeValue, "Wrong value in ConfigParameter::COUNTER_TYPE!");
    auto startEndType = std::get_if<std::string>(&arguments.config[EquiWidthProbeLogicalOperator::ConfigParameters::START_END_TYPE]);
    INVARIANT(startEndType, "Wrong value in ConfigParameter::START_END_TYPE!");

    auto statisticEndTs = arguments.config[LogicalStatisticFields::ConfigParameters::STATISTIC_END_TS];
    auto statisticStartTs = arguments.config[LogicalStatisticFields::ConfigParameters::STATISTIC_START_TS];
    auto statisticHash = arguments.config[LogicalStatisticFields::ConfigParameters::STATISTIC_HASH_FIELD];
    auto statisticNumberOfSeenTuples = arguments.config[LogicalStatisticFields::ConfigParameters::STATISTIC_NUMBER_OF_SEEN_TUPLES];
    if (not std::holds_alternative<NES::SerializableSchema_SerializableField>(statisticEndTs)
        or not std::holds_alternative<NES::SerializableSchema_SerializableField>(statisticStartTs)
        or not std::holds_alternative<NES::SerializableSchema_SerializableField>(statisticHash)
        or not std::holds_alternative<NES::SerializableSchema_SerializableField>(statisticNumberOfSeenTuples))
    {
        throw UnknownLogicalOperator();
    }
    const auto statisticEndTsField
        = SchemaSerializationUtil::deserializeField(std::get<NES::SerializableSchema_SerializableField>(statisticEndTs));
    const auto statisticStartTsField
        = SchemaSerializationUtil::deserializeField(std::get<NES::SerializableSchema_SerializableField>(statisticStartTs));
    const auto statisticHashField
        = SchemaSerializationUtil::deserializeField(std::get<NES::SerializableSchema_SerializableField>(statisticHash));
    const auto statisticNumberOfSeenTuplesField
        = SchemaSerializationUtil::deserializeField(std::get<NES::SerializableSchema_SerializableField>(statisticNumberOfSeenTuples));


    auto logicalOperator = EquiWidthProbeLogicalOperator(
        *statisticHashValue,
        DataTypeProvider::provideDataType(*counterTypeValue),
        DataTypeProvider::provideDataType(*startEndType),
        LogicalStatisticFields{statisticNumberOfSeenTuplesField, statisticHashField, statisticStartTsField, statisticEndTsField});
    return logicalOperator.withInferredSchema(arguments.inputSchemas);
}

}
