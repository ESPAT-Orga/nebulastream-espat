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

#include <Operators/Windows/Aggregations/Sketch/CountMinSketchProbeLogicalOperator.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
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

namespace NES
{

CountMinSketchProbeLogicalOperator::CountMinSketchProbeLogicalOperator(const Statistic::StatisticHash statisticHash, DataType counterType)
    : statisticHash(statisticHash), counterType(counterType)
{
}

CountMinSketchProbeLogicalOperator::CountMinSketchProbeLogicalOperator(
    Statistic::StatisticHash statisticHash,
    DataType counterType,
    std::string rowIndexFieldName,
    std::string columnIndexFieldName,
    std::string counterFieldName,
    LogicalStatisticFields logicalStatisticFields)
    : LogicalStatisticFields(std::move(logicalStatisticFields))
    , statisticHash(statisticHash)
    , counterType(counterType)
    , rowIndexFieldName(std::move(rowIndexFieldName))
    , columnIndexFieldName(std::move(columnIndexFieldName))
    , counterFieldName(std::move(counterFieldName))
{
}

std::string_view CountMinSketchProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool CountMinSketchProbeLogicalOperator::operator==(const CountMinSketchProbeLogicalOperator& rhs) const
{
    return statisticHash == rhs.statisticHash and counterType == rhs.counterType and inputSchema == rhs.inputSchema
        and outputSchema == rhs.outputSchema and inputOriginIds == rhs.inputOriginIds and outputOriginIds == rhs.outputOriginIds;
};

CountMinSketchProbeLogicalOperator CountMinSketchProbeLogicalOperator::withInferredSchema(const std::vector<Schema>& inputSchemas) const
{
    auto copy = *this;
    INVARIANT(inputSchemas.size() == 1, "CountMinProbe should have one input schema but got {}", inputSchemas.size());
    const auto& firstSchema = inputSchemas[0];
    const bool allEqual = std::ranges::adjacent_find(inputSchemas, std::ranges::not_equal_to{}) == inputSchemas.end();
    if (not allEqual)
    {
        throw CannotInferSchema("All input schemas must be equal for CountMinProbe operator");
    }

    /// CountMinSketchProbeLogicalOperator expects the following fields in its input schema. If not, we need to throw
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

    auto addIfMissing = [](std::string s, const std::string& sub) { return s.find(sub) != std::string::npos ? s : sub + s; };

    copy.rowIndexFieldName = addIfMissing(this->rowIndexFieldName, newQualifierForSystemField);
    copy.columnIndexFieldName = addIfMissing(this->columnIndexFieldName, newQualifierForSystemField);
    copy.counterFieldName = addIfMissing(this->counterFieldName, newQualifierForSystemField);

    copy.outputSchema = Schema{};
    copy.statisticHashField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticStartTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticEndTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticNumberOfSeenTuplesField.addQualifierIfNotExists(newQualifierForSystemField);

    copy.outputSchema.addField(copy.statisticHashField);
    copy.outputSchema.addField(copy.statisticStartTsField);
    copy.outputSchema.addField(copy.statisticEndTsField);
    copy.outputSchema.addField(copy.statisticNumberOfSeenTuplesField);

    Schema::Field row(copy.rowIndexFieldName, CountMinSketchProbeLogicalOperator::indexType);
    row.addQualifierIfNotExists(newQualifierForSystemField);
    copy.outputSchema.addField(row);
    Schema::Field column(copy.columnIndexFieldName, CountMinSketchProbeLogicalOperator::indexType);
    column.addQualifierIfNotExists(newQualifierForSystemField);
    copy.outputSchema.addField(column);
    Schema::Field counter(copy.counterFieldName, copy.counterType);
    counter.addQualifierIfNotExists(newQualifierForSystemField);
    copy.outputSchema.addField(counter);

    return copy;
}

CountMinSketchProbeLogicalOperator CountMinSketchProbeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet CountMinSketchProbeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

CountMinSketchProbeLogicalOperator CountMinSketchProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> CountMinSketchProbeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema CountMinSketchProbeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> CountMinSketchProbeLogicalOperator::getChildren() const
{
    return children;
}

std::string CountMinSketchProbeLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("COUNTMIN_PROBE(opId: {}, statHash: {}, counterType: {})", id, statisticHash, counterType);
    }
    return fmt::format("COUNTMIN_PROBE()", statisticHash);
}

void CountMinSketchProbeLogicalOperator::serialize(SerializableOperator& serializableOperator) const
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
    (*serializableOperator.mutable_config())[ConfigParameters::COUNTER_TYPE]
        = descriptorConfigTypeToProto(std::string(magic_enum::enum_name<DataType::Type>(counterType.type)));
    (*serializableOperator.mutable_config())[ConfigParameters::ROW_INDEX_FIELD_NAME] = descriptorConfigTypeToProto(rowIndexFieldName);
    (*serializableOperator.mutable_config())[ConfigParameters::COLUMN_INDEX_FIELD_NAME] = descriptorConfigTypeToProto(columnIndexFieldName);
    (*serializableOperator.mutable_config())[ConfigParameters::COUNTER_FIELD_NAME] = descriptorConfigTypeToProto(counterFieldName);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterCountMinProbeLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto statisticHashValue
        = std::get_if<Statistic::StatisticHash>(&arguments.config[CountMinSketchProbeLogicalOperator::ConfigParameters::STATISTIC_HASH]);
    INVARIANT(statisticHashValue, "Wrong value in ConfigParameter::STATISTIC_HASH");

    auto counterTypeValue = std::get_if<std::string>(&arguments.config[CountMinSketchProbeLogicalOperator::ConfigParameters::COUNTER_TYPE]);
    INVARIANT(counterTypeValue, "Wrong value in ConfigParameter::COUNTER_TYPE");

    auto rowIndexValue = std::get_if<std::string>(&arguments.config[CountMinSketchProbeLogicalOperator::ConfigParameters::ROW_INDEX_FIELD_NAME]);
    INVARIANT(rowIndexValue, "Wrong value in ConfigParameter::ROW_INDEX_FIELD_NAME");

    auto columnIndexValue
        = std::get_if<std::string>(&arguments.config[CountMinSketchProbeLogicalOperator::ConfigParameters::COLUMN_INDEX_FIELD_NAME]);
    INVARIANT(columnIndexValue, "Wrong value in ConfigParameter::COLUMN_INDEX_FIELD_NAME");

    auto counterValue = std::get_if<std::string>(&arguments.config[CountMinSketchProbeLogicalOperator::ConfigParameters::COUNTER_FIELD_NAME]);
    INVARIANT(counterValue, "Wrong value in ConfigParameter::COUNTER_FIELD_NAME");

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


    auto logicalOperator = CountMinSketchProbeLogicalOperator(
        *statisticHashValue,
        DataTypeProvider::provideDataType(*counterTypeValue),
        *rowIndexValue,
        *columnIndexValue,
        *counterValue,
        LogicalStatisticFields{statisticNumberOfSeenTuplesField, statisticHashField, statisticStartTsField, statisticEndTsField});
    return logicalOperator.withInferredSchema(arguments.inputSchemas);
}

}
