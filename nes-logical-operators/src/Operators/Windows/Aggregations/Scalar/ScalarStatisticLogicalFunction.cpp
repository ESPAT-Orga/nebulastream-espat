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

#include <Operators/Windows/Aggregations/Scalar/ScalarStatisticLogicalFunction.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

ScalarStatisticLogicalFunction::ScalarStatisticLogicalFunction(
    const FieldAccessLogicalFunction& onField,
    const FieldAccessLogicalFunction& asField,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType op)
    : statisticId(statisticId)
    , op(op)
    , inputStamp(onField.getDataType())
    , partialAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE))
    , finalAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE))
    , onField(onField)
    , asField(asField)
{
}

std::string_view ScalarStatisticLogicalFunction::getName() const noexcept
{
    return NAME;
}

std::string ScalarStatisticLogicalFunction::toString() const
{
    return fmt::format("ScalarStatistic: op={} onField={} asField={}", magic_enum::enum_name(op), onField, asField);
}

Reflected ScalarStatisticLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<ScalarStatisticLogicalFunction>::operator()(const ScalarStatisticLogicalFunction& function) const
{
    return reflect(detail::ReflectedScalarStatisticLogicalFunction{
        .statisticId = function.statisticId.getRawValue(),
        .op = static_cast<uint64_t>(function.op),
        .onField = function.getOnField(),
        .asField = function.getAsField()});
}

ScalarStatisticLogicalFunction Unreflector<ScalarStatisticLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto data = unreflect<detail::ReflectedScalarStatisticLogicalFunction>(reflected);
    return ScalarStatisticLogicalFunction{
        data.onField, data.asField, StatisticTuple::StatisticId{data.statisticId}, static_cast<StatisticTuple::StatisticType>(data.op)};
}

ScalarStatisticLogicalFunction ScalarStatisticLogicalFunction::withInferredStamp(const Schema& schema) const
{
    auto newOnField = this->getOnField().withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get();
    if (not newOnField.getDataType().isNumeric())
    {
        throw CannotDeserialize("scalar statistics on non numeric fields is not supported, but got {}", newOnField.getDataType());
    }
    /// A scalar statistic persists the bare aggregate, and StatisticTuple carries no null channel to persist a NULL one in
    /// (see makeScalarStatisticRecord). Accepting a nullable field would mean either silently storing a NULL sum as its
    /// raw bytes, or diverging from SUM/AVG, which NULL-poison. Reject it instead of guessing.
    if (newOnField.getDataType().nullable)
    {
        throw CannotDeserialize(
            "scalar statistics on nullable fields are not supported, but got {}. Cast the field or filter out NULLs first",
            newOnField.getDataType());
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
    /// Each op derives from the matching aggregation physical function, which sizes its state from the input stamp and
    /// computes its result in the final stamp (see ScalarStatisticPhysicalFunction.hpp). So take both stamps from the
    /// logical function that op mirrors rather than restating its widening rules here -- e.g. Avg widens the running sum
    /// and returns FLOAT64, without which the average would be an integer division.
    DataType newInputStamp;
    DataType newFinalAggregateStamp;
    switch (op)
    {
        case StatisticTuple::StatisticType::Count: {
            const auto inferred = CountAggregationLogicalFunction{newOnField}.withInferredStamp(schema);
            newInputStamp = inferred.getInputStamp();
            newFinalAggregateStamp = inferred.getFinalAggregateStamp();
            break;
        }
        case StatisticTuple::StatisticType::Sum: {
            const auto inferred = SumAggregationLogicalFunction{newOnField}.withInferredStamp(schema);
            newInputStamp = inferred.getInputStamp();
            newFinalAggregateStamp = inferred.getFinalAggregateStamp();
            break;
        }
        case StatisticTuple::StatisticType::Avg: {
            const auto inferred = AvgAggregationLogicalFunction{newOnField}.withInferredStamp(schema);
            newInputStamp = inferred.getInputStamp();
            newFinalAggregateStamp = inferred.getFinalAggregateStamp();
            break;
        }
        default:
            throw CannotDeserialize("ScalarStatistic expects a scalar statistic type but got {}", magic_enum::enum_name(op));
    }

    auto newAsField = this->getAsField().withFieldName(newAsFieldName).withDataType(newFinalAggregateStamp);
    return this->withOnField(newOnField.withDataType(newInputStamp))
        .withInputStamp(newInputStamp)
        .withFinalAggregateStamp(newFinalAggregateStamp)
        .withAsField(newAsField);
}

DataType ScalarStatisticLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType ScalarStatisticLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType ScalarStatisticLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction ScalarStatisticLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction ScalarStatisticLogicalFunction::getAsField() const
{
    return asField;
}

ScalarStatisticLogicalFunction ScalarStatisticLogicalFunction::withInputStamp(DataType newInputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(newInputStamp);
    return copy;
}

ScalarStatisticLogicalFunction ScalarStatisticLogicalFunction::withPartialAggregateStamp(DataType newPartialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(newPartialAggregateStamp);
    return copy;
}

ScalarStatisticLogicalFunction ScalarStatisticLogicalFunction::withFinalAggregateStamp(DataType newFinalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(newFinalAggregateStamp);
    return copy;
}

ScalarStatisticLogicalFunction ScalarStatisticLogicalFunction::withOnField(FieldAccessLogicalFunction newOnField) const
{
    auto copy = *this;
    copy.onField = std::move(newOnField);
    return copy;
}

ScalarStatisticLogicalFunction ScalarStatisticLogicalFunction::withAsField(FieldAccessLogicalFunction newAsField) const
{
    auto copy = *this;
    copy.asField = std::move(newAsField);
    return copy;
}

bool ScalarStatisticLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

bool ScalarStatisticLogicalFunction::operator==(const ScalarStatisticLogicalFunction& rhs) const
{
    return this->getName() == rhs.getName() && this->onField == rhs.onField && this->asField == rhs.asField
        && this->statisticId == rhs.statisticId && this->op == rhs.op;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterScalarStatisticAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<ScalarStatisticLogicalFunction>(arguments.reflected));
    }
    throw NotImplemented("ScalarStatisticLogicalFunction can only be reconstructed from its reflected form");
}

}
