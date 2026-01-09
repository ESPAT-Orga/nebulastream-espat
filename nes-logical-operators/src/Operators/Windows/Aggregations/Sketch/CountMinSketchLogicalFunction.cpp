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
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

CountMinSketchLogicalFunction::CountMinSketchLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    const uint64_t columns,
    const uint64_t rows,
    const uint64_t seed,
    const DataType counterType,
    const Statistic::StatisticHash statisticHash)
    : columns(columns)
    , rows(rows)
    , seed(seed)
    , counterType(counterType)
    , statisticHash(statisticHash)
    , inputStamp(onField.getDataType())
    , partialAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE))
    , finalAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE))
    , onField(onField)
    , asField(onField)
{
}

CountMinSketchLogicalFunction::CountMinSketchLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    const FieldAccessLogicalFunction& asField,
    const uint64_t columns,
    const uint64_t rows,
    const uint64_t seed,
    const DataType counterType,
    const Statistic::StatisticHash statisticHash)
    : columns(columns)
    , rows(rows)
    , seed(seed)
    , counterType(counterType)
    , statisticHash(statisticHash)
    , inputStamp(onField.getDataType())
    , partialAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE))
    , finalAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE))
    , onField(onField)
    , asField(asField)
{
}

std::string_view CountMinSketchLogicalFunction::getName() const noexcept
{
    return NAME;
}

std::string CountMinSketchLogicalFunction::toString() const
{
    return fmt::format("CountMinSketch: onField={} asField={} columns={} rows={}", onField, asField, columns, rows);
}

Reflected CountMinSketchLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<CountMinSketchLogicalFunction>::operator()(const CountMinSketchLogicalFunction& op) const
{
    return reflect(detail::ReflectedCountMinSketchLogicalFunction{
        .statisticHash = op.statisticHash,
        .onField = op.getOnField(),
        .asField = op.getAsField(),
        .columns = op.columns,
        .rows = op.rows,
        .seed = op.seed,
        .counterType = op.counterType});
}

CountMinSketchLogicalFunction Unreflector<CountMinSketchLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto data = unreflect<detail::ReflectedCountMinSketchLogicalFunction>(reflected);
    return CountMinSketchLogicalFunction{
        data.onField, data.asField, data.columns, data.rows, data.seed, data.counterType, data.statisticHash};
}

CountMinSketchLogicalFunction CountMinSketchLogicalFunction::withInferredStamp(const Schema& schema) const
{
    auto newOnField = this->getOnField().withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get();
    if (not newOnField.getDataType().isNumeric())
    {
        throw CannotDeserialize("count min on non numeric fields is not supported, but got {}", newOnField.getDataType());
    }

    const auto onFieldName = newOnField.getFieldName();
    const auto asFieldName = this->getAsField().getFieldName();
    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);

    std::string newAsFieldName;
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        newAsFieldName = attributeNameResolver + asFieldName;
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        newAsFieldName = attributeNameResolver + fieldName;
    }
    auto newAsField = this->getAsField().withFieldName(newAsFieldName).withDataType(newOnField.getDataType());
    return this->withOnField(newOnField)
        .withInputStamp(newOnField.getDataType())
        .withFinalAggregateStamp(newOnField.getDataType())
        .withAsField(newAsField);
}

DataType CountMinSketchLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType CountMinSketchLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType CountMinSketchLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction CountMinSketchLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction CountMinSketchLogicalFunction::getAsField() const
{
    return asField;
}

CountMinSketchLogicalFunction CountMinSketchLogicalFunction::withInputStamp(DataType newInputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(newInputStamp);
    return copy;
}

CountMinSketchLogicalFunction CountMinSketchLogicalFunction::withPartialAggregateStamp(DataType newPartialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(newPartialAggregateStamp);
    return copy;
}

CountMinSketchLogicalFunction CountMinSketchLogicalFunction::withFinalAggregateStamp(DataType newFinalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(newFinalAggregateStamp);
    return copy;
}

CountMinSketchLogicalFunction CountMinSketchLogicalFunction::withOnField(FieldAccessLogicalFunction newOnField) const
{
    auto copy = *this;
    copy.onField = std::move(newOnField);
    return copy;
}

CountMinSketchLogicalFunction CountMinSketchLogicalFunction::withAsField(FieldAccessLogicalFunction newAsField) const
{
    auto copy = *this;
    copy.asField = std::move(newAsField);
    return copy;
}

bool CountMinSketchLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

bool CountMinSketchLogicalFunction::operator==(const CountMinSketchLogicalFunction& rhs) const
{
    return this->getName() == rhs.getName() && this->onField == rhs.onField && this->asField == rhs.asField && this->columns == rhs.columns
        && this->rows == rhs.rows;
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterCountMinSketchAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<CountMinSketchLogicalFunction>(arguments.reflected));
    }
    PRECONDITION(false, "Expected arguments are missing");
    std::unreachable();
}

}
