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
/// Per-bucket cost of the EquiWidthHistogram physical layout: one uint64 counter + two uint64 bounds.
constexpr uint64_t kHistogramBucketBytes = sizeof(uint64_t) * 3;
/// Fixed metadata overhead in the physical layout.
constexpr uint64_t kHistogramOverheadBytes = sizeof(uint64_t);
}

EquiWidthHistogramLogicalFunction::EquiWidthHistogramLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    const uint64_t memoryBudget,
    const uint64_t minValue,
    const uint64_t maxValue,
    const Statistic::StatisticId statisticId)
    : StatisticLogicalFunction(memoryBudget)
    , minValue(minValue)
    , maxValue(maxValue)
    , statisticId(statisticId)
    , inputStamp(onField.getDataType())
    , partialAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE))
    , finalAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE))
    , onField(onField)
    , asField(onField)
{
}

EquiWidthHistogramLogicalFunction::EquiWidthHistogramLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    const FieldAccessLogicalFunction& asField,
    const uint64_t memoryBudget,
    const uint64_t minValue,
    const uint64_t maxValue,
    const Statistic::StatisticId statisticId)
    : StatisticLogicalFunction(memoryBudget)
    , minValue(minValue)
    , maxValue(maxValue)
    , statisticId(statisticId)
    , inputStamp(onField.getDataType())
    , partialAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE))
    , finalAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE))
    , onField(onField)
    , asField(asField)
{
}

std::string_view EquiWidthHistogramLogicalFunction::getName() const noexcept
{
    return NAME;
}

std::string EquiWidthHistogramLogicalFunction::toString() const
{
    return fmt::format(
        "EquiWidthHistogram: onField={} asField={} memoryBudget={} minValue={} maxValue={}",
        onField,
        asField,
        memoryBudget,
        minValue,
        maxValue);
}

Reflected EquiWidthHistogramLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<EquiWidthHistogramLogicalFunction>::operator()(const EquiWidthHistogramLogicalFunction& function) const
{
    return reflect(detail::ReflectedEquiWidthHistogramLogicalFunction{
        .onField = function.getOnField(),
        .asField = function.getAsField(),
        .memoryBudget = function.memoryBudget,
        .minValue = function.minValue,
        .maxValue = function.maxValue,
        .statisticId = function.statisticId.getRawValue()});
}

EquiWidthHistogramLogicalFunction Unreflector<EquiWidthHistogramLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto data = unreflect<detail::ReflectedEquiWidthHistogramLogicalFunction>(reflected);
    return EquiWidthHistogramLogicalFunction{
        data.onField, data.asField, data.memoryBudget, data.minValue, data.maxValue, Statistic::StatisticId{data.statisticId}};
}

EquiWidthHistogramLogicalFunction EquiWidthHistogramLogicalFunction::withInferredStamp(const Schema& schema) const
{
    auto newOnField = this->getOnField().withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get();
    if (not newOnField.getDataType().isNumeric())
    {
        throw CannotDeserialize("equi width histogram on non numeric fields is not supported, but got {}", newOnField.getDataType());
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

DataType EquiWidthHistogramLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType EquiWidthHistogramLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType EquiWidthHistogramLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction EquiWidthHistogramLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction EquiWidthHistogramLogicalFunction::getAsField() const
{
    return asField;
}

EquiWidthHistogramLogicalFunction EquiWidthHistogramLogicalFunction::withInputStamp(DataType newInputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(newInputStamp);
    return copy;
}

EquiWidthHistogramLogicalFunction EquiWidthHistogramLogicalFunction::withPartialAggregateStamp(DataType newPartialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(newPartialAggregateStamp);
    return copy;
}

EquiWidthHistogramLogicalFunction EquiWidthHistogramLogicalFunction::withFinalAggregateStamp(DataType newFinalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(newFinalAggregateStamp);
    return copy;
}

EquiWidthHistogramLogicalFunction EquiWidthHistogramLogicalFunction::withOnField(FieldAccessLogicalFunction newOnField) const
{
    auto copy = *this;
    copy.onField = std::move(newOnField);
    return copy;
}

EquiWidthHistogramLogicalFunction EquiWidthHistogramLogicalFunction::withAsField(FieldAccessLogicalFunction newAsField) const
{
    auto copy = *this;
    copy.asField = std::move(newAsField);
    return copy;
}

bool EquiWidthHistogramLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

bool EquiWidthHistogramLogicalFunction::operator==(const EquiWidthHistogramLogicalFunction& rhs) const
{
    return this->getName() == rhs.getName() && this->onField == rhs.onField && this->asField == rhs.asField
        && this->memoryBudget == rhs.memoryBudget && this->minValue == rhs.minValue && this->maxValue == rhs.maxValue;
}

std::unique_ptr<StatisticConfig> EquiWidthHistogramLogicalFunction::calculateConfigs() const
{
    const uint64_t available = memoryBudget > kHistogramOverheadBytes ? memoryBudget - kHistogramOverheadBytes : 0;
    const uint64_t numBuckets = std::max<uint64_t>(1, available / kHistogramBucketBytes);
    return std::make_unique<EquiWidthHistogramConfig>(
        numBuckets, minValue, maxValue, DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE));
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterEquiWidthHistogramAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<EquiWidthHistogramLogicalFunction>(arguments.reflected));
    }
    /// We assume the fields vector starts with onField, asField
    PRECONDITION(arguments.fields.size() >= 2, "EquiWidthHistogramLogicalFunction requires onField and asField");
    PRECONDITION(arguments.memoryBudget.has_value(), "EquiWidthHistogramLogicalFunction requires memoryBudget to be set");
    PRECONDITION(arguments.histogramMinValue.has_value(), "EquiWidthHistogramLogicalFunction requires min value to be set");
    PRECONDITION(arguments.histogramMaxValue.has_value(), "EquiWidthHistogramLogicalFunction requires max value be set");
    PRECONDITION(arguments.statisticId.has_value(), "EquiWidthHistogramLogicalFunction requires statisticId to be set");

    return std::make_shared<WindowAggregationLogicalFunction>(EquiWidthHistogramLogicalFunction{
        arguments.fields[0],
        arguments.fields[1],
        arguments.memoryBudget.value(),
        arguments.histogramMinValue.value(),
        arguments.histogramMaxValue.value(),
        arguments.statisticId.value()});
}

}
