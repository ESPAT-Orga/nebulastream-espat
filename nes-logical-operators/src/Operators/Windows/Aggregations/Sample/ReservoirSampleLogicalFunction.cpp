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
constexpr uint64_t kReservoirSeed = 42;
}

ReservoirSampleLogicalFunction::ReservoirSampleLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    std::vector<FieldAccessLogicalFunction> sampleFields,
    const uint64_t memoryBudget,
    const Statistic::StatisticId statisticId)
    : StatisticLogicalFunction(memoryBudget)
    , sampleFields(std::move(sampleFields))
    , statisticId(statisticId)
    , inputStamp(onField.getDataType())
    , partialAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE))
    , finalAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE))
    , onField(onField)
    , asField(onField)
{
}

ReservoirSampleLogicalFunction::ReservoirSampleLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    const FieldAccessLogicalFunction& asField,
    std::vector<FieldAccessLogicalFunction> sampleFields,
    const uint64_t memoryBudget,
    const Statistic::StatisticId statisticId)
    : StatisticLogicalFunction(memoryBudget)
    , sampleFields(std::move(sampleFields))
    , statisticId(statisticId)
    , inputStamp(onField.getDataType())
    , partialAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE))
    , finalAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE))
    , onField(onField)
    , asField(asField)
{
}

std::string_view ReservoirSampleLogicalFunction::getName() const noexcept
{
    return NAME;
}

std::string ReservoirSampleLogicalFunction::toString() const
{
    return fmt::format("ReservoirSample: onField={} asField={} memoryBudget={}", onField, asField, memoryBudget);
}

Reflected ReservoirSampleLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<ReservoirSampleLogicalFunction>::operator()(const ReservoirSampleLogicalFunction& function) const
{
    return reflect(detail::ReflectedReservoirSampleLogicalFunction{
        .onField = function.getOnField(),
        .asField = function.getAsField(),
        .sampleFields = function.sampleFields,
        .memoryBudget = function.memoryBudget,
        .statisticId = function.statisticId.getRawValue()});
}

ReservoirSampleLogicalFunction Unreflector<ReservoirSampleLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto data = unreflect<detail::ReflectedReservoirSampleLogicalFunction>(reflected);
    return ReservoirSampleLogicalFunction{
        data.onField, data.asField, data.sampleFields, data.memoryBudget, Statistic::StatisticId{data.statisticId}};
}

ReservoirSampleLogicalFunction ReservoirSampleLogicalFunction::withInferredStamp(const Schema& schema) const
{
    auto newOnField = this->getOnField().withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get();

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

DataType ReservoirSampleLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType ReservoirSampleLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType ReservoirSampleLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction ReservoirSampleLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction ReservoirSampleLogicalFunction::getAsField() const
{
    return asField;
}

ReservoirSampleLogicalFunction ReservoirSampleLogicalFunction::withInputStamp(DataType newInputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(newInputStamp);
    return copy;
}

ReservoirSampleLogicalFunction ReservoirSampleLogicalFunction::withPartialAggregateStamp(DataType newPartialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(newPartialAggregateStamp);
    return copy;
}

ReservoirSampleLogicalFunction ReservoirSampleLogicalFunction::withFinalAggregateStamp(DataType newFinalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(newFinalAggregateStamp);
    return copy;
}

ReservoirSampleLogicalFunction ReservoirSampleLogicalFunction::withOnField(FieldAccessLogicalFunction newOnField) const
{
    auto copy = *this;
    copy.onField = std::move(newOnField);
    return copy;
}

ReservoirSampleLogicalFunction ReservoirSampleLogicalFunction::withAsField(FieldAccessLogicalFunction newAsField) const
{
    auto copy = *this;
    copy.asField = std::move(newAsField);
    return copy;
}

bool ReservoirSampleLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

bool ReservoirSampleLogicalFunction::operator==(const ReservoirSampleLogicalFunction& rhs) const
{
    return this->getName() == rhs.getName() && this->onField == rhs.onField && this->asField == rhs.asField
        && this->memoryBudget == rhs.memoryBudget && this->statisticId == rhs.statisticId;
}

std::unique_ptr<StatisticConfig> ReservoirSampleLogicalFunction::calculateConfigs() const
{
    /// Map the budget onto a record count using the input-stamp size as the per-record cost.
    /// We use the input stamp (not the sum of sampleFields) because sampleField stamps may not be
    /// inferred yet at calculation time, and the input stamp gives a deterministic, schema-agnostic mapping.
    uint64_t recordSize = inputStamp.getSizeInBytesWithoutNull();
    if (recordSize == 0)
    {
        recordSize = sizeof(uint64_t);
    }
    return std::make_unique<ReservoirSampleConfig>(std::max<uint64_t>(1, memoryBudget / recordSize), kReservoirSeed);
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterReservoirSampleAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<ReservoirSampleLogicalFunction>(arguments.reflected));
    }
    /// We assume the fields vector starts with onField (useless), asField, and then has the sampleFields
    PRECONDITION(
        arguments.fields.size() >= 3,
        "ReservoirSampleLogicalFunction requires onField (even though unused), asField, and at least one field for the sample");
    PRECONDITION(arguments.memoryBudget.has_value(), "ReservoirSampleLogicalFunction requires memoryBudget to be set!");
    PRECONDITION(arguments.statisticId.has_value(), "ReservoirSampleLogicalFunction requires statisticId to be set!");

    const std::vector<FieldAccessLogicalFunction> sampleFields{
        std::make_move_iterator(arguments.fields.begin() + 2), std::make_move_iterator(arguments.fields.end())};
    return std::make_shared<WindowAggregationLogicalFunction>(ReservoirSampleLogicalFunction{
        arguments.fields[0], arguments.fields[1], sampleFields, arguments.memoryBudget.value(), arguments.statisticId.value()});
}
}
