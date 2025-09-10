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

#include <Operators/Windows/Aggregations/Synopsis/Sketch/CountMinSketchLogicalFunction.hpp>

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
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

CountMinSketchLogicalFunction::CountMinSketchLogicalFunction(
    const FieldAccessLogicalFunction& onField, uint64_t columns, uint64_t rows)
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
    const FieldAccessLogicalFunction& onField,
    const FieldAccessLogicalFunction& asField,
    uint64_t columns,
    uint64_t rows)
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

std::shared_ptr<WindowAggregationLogicalFunction> CountMinSketchLogicalFunction::create(
    const FieldAccessLogicalFunction& onField,
    const FieldAccessLogicalFunction& asField,
    uint64_t columns,
    uint64_t rows)
{
    return std::make_shared<CountMinSketchLogicalFunction>(onField, asField, columns, rows);
}

std::shared_ptr<WindowAggregationLogicalFunction> CountMinSketchLogicalFunction::create(
    const FieldAccessLogicalFunction& onField, uint64_t columns, uint64_t rows)
{
    return std::make_shared<CountMinSketchLogicalFunction>(onField, columns, rows);
}

std::string_view CountMinSketchLogicalFunction::getName() const noexcept
{
    return NAME;
}

// TODO Remove when not necessary anymore in upstream NES.
void CountMinSketchLogicalFunction::inferStamp(const Schema& schema)
{
    (void)schema;
    /// We first infer the dataType of the input field and set the output dataType as the same.
    ///Set fully qualified name for the as Field
    const auto onFieldName = onField.getFieldName();
    const auto asFieldName = asField.getFieldName();

    const auto attributeNameResolver = "stream$";
    if (onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        onField = onField.withFieldName(attributeNameResolver + onFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        const auto fieldName = onFieldName.substr(onFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        onField = onField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }
    inputStamp = onField.getDataType();
    finalAggregateStamp = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    asField = asField.withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
}

NES::SerializableAggregationFunction CountMinSketchLogicalFunction::serialize() const
{
    /// TODO Adapt to count min sketch
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(onField.serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(asField.serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);
    return serializedAggregationFunction;
}

/// TODO Implement
AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterCountMinSketchAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    (void)arguments;
    PRECONDITION(false, "TODO");
}

}
