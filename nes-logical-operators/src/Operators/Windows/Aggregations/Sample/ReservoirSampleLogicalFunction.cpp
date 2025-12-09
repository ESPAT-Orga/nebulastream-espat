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

#include <Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

ReservoirSampleLogicalFunction::ReservoirSampleLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    std::vector<FieldAccessLogicalFunction> sampleFields,
    const uint64_t reservoirSize,
    const uint64_t sampleHash)
    : WindowAggregationLogicalFunction(
          onField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          onField)
    , sampleFields(std::move(sampleFields))
    , reservoirSize(reservoirSize)
    , sampleHash(sampleHash)
{
}

ReservoirSampleLogicalFunction::ReservoirSampleLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    const FieldAccessLogicalFunction& asField,
    std::vector<FieldAccessLogicalFunction> sampleFields,
    const uint64_t reservoirSize,
    const uint64_t sampleHash)
    : WindowAggregationLogicalFunction(
          onField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          onField,
          asField)
    , sampleFields(std::move(sampleFields))
    , reservoirSize(reservoirSize)
    , sampleHash(sampleHash)
{
}

std::string_view ReservoirSampleLogicalFunction::getName() const noexcept
{
    return NAME;
}

/// Remove this function when upstream removes it.
void ReservoirSampleLogicalFunction::inferStamp(const Schema& schema)
{
    if (const auto sourceNameQualifier = schema.getSourceNameQualifier())
    {
        this->setOnField(this->getOnField().withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get());

        const auto attributeNameResolver = sourceNameQualifier.value() + std::string(Schema::ATTRIBUTE_NAME_SEPARATOR);
        const auto asFieldName = this->getAsField().getFieldName();

        ///If on and as field name are different then append the attribute name resolver from on field to the as field
        if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
        {
            this->setAsField(this->getAsField().withFieldName(attributeNameResolver + asFieldName));
        }
        else
        {
            const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
            this->setAsField(this->getAsField().withFieldName(attributeNameResolver + fieldName));
        }
        this->setInputStamp(this->getOnField().getDataType());
        this->setFinalAggregateStamp(this->getOnField().getDataType());
        this->setAsField(this->getAsField().withDataType(getFinalAggregateStamp()));
    }
    else
    {
        throw CannotInferSchema("Schema lacked source name qualifier: {}", schema);
    }
}

NES::SerializableAggregationFunction ReservoirSampleLogicalFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(this->getOnField().serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(this->getAsField().serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);

    FunctionList fnList;
    for (auto field : sampleFields)
    {
        auto* addPtr = fnList.add_functions();
        addPtr->CopyFrom(field.serialize());
    }
    serializedAggregationFunction.mutable_sample_fields()->CopyFrom(fnList);
    serializedAggregationFunction.set_reservoir_size(reservoirSize);

    serializedAggregationFunction.set_sample_hash(sampleHash);

    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterReservoirSampleAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    /// We assume the fields vector starts with onField (useless), asField, and then has the sampleFields
    PRECONDITION(
        arguments.fields.size() >= 3,
        "ReservoirSampleLogicalFunction requires onField (even though unused), asField, and at least one field for the sample");
    PRECONDITION(arguments.reservoirSize.has_value(), "ReservoirSampleLogicalFunction requires reservoirSize to be set!");
    PRECONDITION(arguments.sampleHash.has_value(), "ReservoirSampleLogicalFunction requires statisticHash to be set!");

    const std::vector<FieldAccessLogicalFunction> sampleFields{
        std::make_move_iterator(arguments.fields.begin() + 2), std::make_move_iterator(arguments.fields.end())};
    const auto reservoirSample = std::make_shared<ReservoirSampleLogicalFunction>(
        arguments.fields[0], arguments.fields[1], sampleFields, arguments.reservoirSize.value(), arguments.sampleHash.value());
    return reservoirSample;
}
}
