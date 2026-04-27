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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/SketchDimensions.hpp>
#include <Operators/Windows/Aggregations/StatisticLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <Statistic.hpp>

namespace NES
{

namespace
{
constexpr uint64_t kCountMinSeed = 42;
constexpr uint64_t kCountMinRows = 3;
}

CountMinSketchLogicalFunction::CountMinSketchLogicalFunction(
    const FieldAccessLogicalFunction& onField, const uint64_t memoryBudget, const Statistic::StatisticId statisticId)
    : StatisticLogicalFunction(memoryBudget)
    , statisticId(statisticId)
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
    const uint64_t memoryBudget,
    const Statistic::StatisticId statisticId)
    : StatisticLogicalFunction(memoryBudget)
    , statisticId(statisticId)
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
    return fmt::format("CountMinSketch: onField={} asField={} memoryBudget={}", onField, asField, memoryBudget);
}

Reflected CountMinSketchLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<CountMinSketchLogicalFunction>::operator()(const CountMinSketchLogicalFunction& op) const
{
    return reflect(detail::ReflectedCountMinSketchLogicalFunction{
        .statisticId = op.statisticId.getRawValue(),
        .onField = op.getOnField(),
        .asField = op.getAsField(),
        .memoryBudget = op.memoryBudget});
}

CountMinSketchLogicalFunction Unreflector<CountMinSketchLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto data = unreflect<detail::ReflectedCountMinSketchLogicalFunction>(reflected);
    return CountMinSketchLogicalFunction{data.onField, data.asField, data.memoryBudget, Statistic::StatisticId{data.statisticId}};
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
    return this->getName() == rhs.getName() && this->onField == rhs.onField && this->asField == rhs.asField
        && this->memoryBudget == rhs.memoryBudget && this->statisticId == rhs.statisticId;
}

std::unique_ptr<StatisticConfig> CountMinSketchLogicalFunction::calculateConfigs() const
{
    constexpr uint64_t counterBytes = sizeof(uint64_t);
    const uint64_t cols = std::max<uint64_t>(1, memoryBudget / (kCountMinRows * counterBytes));
    return std::make_unique<CountMinSketchConfig>(
        NumberOfRows{kCountMinRows},
        NumberOfCols{cols},
        kCountMinSeed,
        DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE));
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterCountMinSketchAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<CountMinSketchLogicalFunction>(arguments.reflected));
    }
    PRECONDITION(arguments.fields.size() >= 2, "CountMinSketchLogicalFunction requires onField and asField");
    PRECONDITION(arguments.memoryBudget.has_value(), "CountMinSketchLogicalFunction requires memoryBudget to be set");
    PRECONDITION(arguments.statisticId.has_value(), "CountMinSketchLogicalFunction requires statisticId to be set");

    return std::make_shared<WindowAggregationLogicalFunction>(CountMinSketchLogicalFunction{
        arguments.fields[0], arguments.fields[1], arguments.memoryBudget.value(), arguments.statisticId.value()});
}

}
