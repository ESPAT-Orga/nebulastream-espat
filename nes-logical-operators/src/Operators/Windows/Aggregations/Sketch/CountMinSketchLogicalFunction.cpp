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

#include <Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp>

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

CountMinSketchLogicalFunction::CountMinSketchLogicalFunction(
    const FieldAccessLogicalFunction& onField, const uint64_t columns, const uint64_t rows)
    : WindowAggregationLogicalFunction(
          onField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          onField)
    , columns(columns)
    , rows(rows)
{
}

CountMinSketchLogicalFunction::CountMinSketchLogicalFunction(
    const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField, const uint64_t columns, const uint64_t rows)
    : WindowAggregationLogicalFunction(
          onField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          onField,
          asField)
    , columns(columns)
    , rows(rows)
{
}

std::string_view CountMinSketchLogicalFunction::getName() const noexcept
{
    return NAME;
}

/// Remove when not necessary anymore in upstream NES.
void CountMinSketchLogicalFunction::inferStamp(const Schema&)
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    ///Set fully qualified name for the as Field
    const auto onFieldName = getOnField().getFieldName();
    const auto asFieldName = getAsField().getFieldName();

    const auto attributeNameResolver = "stream$";
    if (onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        this->setOnField(getOnField().withFieldName(attributeNameResolver + onFieldName).get<FieldAccessLogicalFunction>());
    }
    else
    {
        const auto fieldName = onFieldName.substr(onFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        this->setOnField(getOnField().withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>());
    }
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        this->setAsField(getAsField().withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>());
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        this->setAsField(getAsField().withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>());
    }
    this->setInputStamp(this->getOnField().getDataType());
    this->setFinalAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    this->setAsField(this->getAsField().withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>());
}

NES::SerializableAggregationFunction CountMinSketchLogicalFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(this->getOnField().serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(this->getAsField().serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);
    serializedAggregationFunction.set_count_min_num_columns(columns);
    serializedAggregationFunction.set_count_min_num_rows(rows);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterCountMinSketchAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    /// We assume the fields vector starts with onField, asField
    PRECONDITION(arguments.fields.size() >= 2, "CountMinSketchLogicalFunction requires onField and asField");
    PRECONDITION(arguments.countMinNumColumns.has_value(), "CountMinSketchLogicalFunction requires number of columns to be set!");
    PRECONDITION(arguments.countMinNumRows.has_value(), "CountMinSketchLogicalFunction requires number of rows to be set!");
    PRECONDITION(arguments.numberOfSeenTuplesField.has_value(), "CountMinSketchLogicalFunction requires number of seen tuples be set!");

    const auto countMin = std::make_shared<CountMinSketchLogicalFunction>(
        arguments.fields[0], arguments.fields[1], arguments.countMinNumColumns.value(), arguments.countMinNumRows.value());
    return countMin;
}

}
