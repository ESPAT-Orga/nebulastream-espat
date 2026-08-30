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
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <magic_enum/magic_enum.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

ScalarStatisticLogicalFunction::ScalarStatisticLogicalFunction(
    AggregationFieldAccess inputFunction,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType op)
    : statisticId(statisticId)
    , op(op)
    , inputFunction(std::move(inputFunction))
    , aggregateType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE))
{
}

ScalarStatisticLogicalFunction::ScalarStatisticLogicalFunction(
    AggregationFieldAccess inputFunction,
    DataType aggregateType,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType op)
    : statisticId(statisticId)
    , op(op)
    , inputFunction(std::move(inputFunction))
    , aggregateType(std::move(aggregateType))
{
}

std::string_view ScalarStatisticLogicalFunction::getName() const noexcept
{
    return NAME;
}

std::string ScalarStatisticLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}({})", NAME, magic_enum::enum_name(op));
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({}, {})", NAME, magic_enum::enum_name(op), inputExplain);
}

bool ScalarStatisticLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

DataType ScalarStatisticLogicalFunction::getAggregateType() const
{
    return aggregateType;
}

AggregationFieldAccess ScalarStatisticLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

ScalarStatisticLogicalFunction ScalarStatisticLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    const auto newInputFunction = inferFieldAccess(inputFunction, schema);
    if (!newInputFunction->getDataType().isNumeric())
    {
        throw CannotInferStamp("scalar statistics on non numeric fields is not supported, but got {}", newInputFunction->getDataType());
    }
    if (newInputFunction->getDataType().nullable)
    {
        throw CannotInferStamp(
            "scalar statistics on nullable fields are not supported, but got {}. Cast the field or filter out NULLs first",
            newInputFunction->getDataType());
    }

    DataType newAggregateType;
    switch (op)
    {
        case StatisticTuple::StatisticType::Count:
            newAggregateType = CountAggregationLogicalFunction{newInputFunction, false}.withInferredType(schema).getAggregateType();
            break;
        case StatisticTuple::StatisticType::Sum:
            newAggregateType = SumAggregationLogicalFunction{newInputFunction}.withInferredType(schema).getAggregateType();
            break;
        case StatisticTuple::StatisticType::Avg:
            newAggregateType = AvgAggregationLogicalFunction{newInputFunction}.withInferredType(schema).getAggregateType();
            break;
        default:
            throw CannotInferStamp("ScalarStatistic expects a scalar statistic type but got {}", magic_enum::enum_name(op));
    }

    return ScalarStatisticLogicalFunction{newInputFunction, newAggregateType, statisticId, op};
}

bool ScalarStatisticLogicalFunction::operator==(const ScalarStatisticLogicalFunction& rhs) const
{
    return this->getName() == rhs.getName() && this->inputFunction == rhs.inputFunction && this->statisticId == rhs.statisticId
        && this->op == rhs.op && this->aggregateType == rhs.aggregateType;
}

Reflected Reflector<ScalarStatisticLogicalFunction>::operator()(
    const ScalarStatisticLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedScalarStatisticLogicalFunction{
        .statisticId = function.statisticId.getRawValue(),
        .op = static_cast<uint64_t>(function.op),
        .inputFunction = function.getInputFunction()});
}

ScalarStatisticLogicalFunction Unreflector<ScalarStatisticLogicalFunction>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto data = context.unreflect<detail::ReflectedScalarStatisticLogicalFunction>(reflected);
    return ScalarStatisticLogicalFunction{
        std::move(data.inputFunction),
        StatisticTuple::StatisticId{data.statisticId},
        static_cast<StatisticTuple::StatisticType>(data.op)};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterScalarStatisticAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments)
{
    throw NotImplemented("ScalarStatisticLogicalFunction can only be reconstructed from its reflected form");
}

}

size_t std::hash<NES::ScalarStatisticLogicalFunction>::operator()(const NES::ScalarStatisticLogicalFunction& function) const noexcept
{
    return folly::hash::hash_combine(function.getInputFunction(), function.getName());
}
