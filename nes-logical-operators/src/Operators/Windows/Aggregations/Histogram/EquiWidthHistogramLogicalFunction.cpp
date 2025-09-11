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

#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>

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
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

EquiWidthHistogramLogicalFunction::EquiWidthHistogramLogicalFunction(
    const FieldAccessLogicalFunction& onField, const uint64_t numBuckets, const uint64_t minValue, const uint64_t maxValue)
    : WindowAggregationLogicalFunction(
          onField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          onField)
    , numBuckets(numBuckets)
    , minValue(minValue)
    , maxValue(maxValue)
{
}

EquiWidthHistogramLogicalFunction::EquiWidthHistogramLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    const FieldAccessLogicalFunction& asField,
    const uint64_t numBuckets,
    const uint64_t minValue,
    const uint64_t maxValue)
    : WindowAggregationLogicalFunction(
          onField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          onField,
          asField)
    , numBuckets(numBuckets)
    , minValue(minValue)
    , maxValue(maxValue)
{
}

std::string_view EquiWidthHistogramLogicalFunction::getName() const noexcept
{
    return NAME;
}

///  Remove when not necessary anymore in upstream NES.
void EquiWidthHistogramLogicalFunction::inferStamp(const Schema&)
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

NES::SerializableAggregationFunction EquiWidthHistogramLogicalFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(this->getOnField().serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(this->getAsField().serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);
    serializedAggregationFunction.set_histogram_num_buckets(numBuckets);
    serializedAggregationFunction.set_histogram_min_value(minValue);
    serializedAggregationFunction.set_histogram_max_value(maxValue);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterEquiWidthHistogramAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    /// We assume the fields vector starts with onField, asField
    PRECONDITION(arguments.fields.size() >= 2, "EquiWidthHistogramLogicalFunction requires onField and asField");
    PRECONDITION(arguments.histogramMinValue.has_value(), "EquiWidthHistogramLogicalFunction requires min value to be set!");
    PRECONDITION(arguments.histogramMaxValue.has_value(), "EquiWidthHistogramLogicalFunction requires max value be set!");
    PRECONDITION(arguments.histogramNumBuckets.has_value(), "EquiWidthHistogramLogicalFunction requires number of buckets to be set!");

    const auto equiWidthHist = std::make_shared<EquiWidthHistogramLogicalFunction>(
        arguments.fields[0],
        arguments.fields[1],
        arguments.histogramNumBuckets.value(),
        arguments.histogramMinValue.value(),
        arguments.histogramMaxValue.value());
    return equiWidthHist;
}

}
