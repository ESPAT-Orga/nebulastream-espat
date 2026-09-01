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

#include <Operators/Windows/Aggregations/ScalarStatisticAggregationLogicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

ScalarStatisticAggregationLogicalFunction::ScalarStatisticAggregationLogicalFunction(
    AggregationFieldAccess inputFunction, const StatisticId statisticId, const StatisticType op)
    : inputFunction(std::move(inputFunction)), statisticId(statisticId), op(op)
{
}

AggregationFieldAccess ScalarStatisticAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

StatisticId ScalarStatisticAggregationLogicalFunction::getStatisticId() const
{
    return statisticId;
}

StatisticType ScalarStatisticAggregationLogicalFunction::getOp() const
{
    return op;
}

std::string_view ScalarStatisticAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

DataType ScalarStatisticAggregationLogicalFunction::getAggregateType() const
{
    /// The payload handed to StatisticStoreWriter, which reads it as VariableSizedData. NOT_NULLABLE because
    /// the writer rejects nullable inputs and the stored payload has no null bit.
    return DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE);
}

bool ScalarStatisticAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return false;
}

std::string ScalarStatisticAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}({})", NAME, magic_enum::enum_name(op));
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({}, {}, statisticId: {})", NAME, magic_enum::enum_name(op), inputExplain, statisticId.getRawValue());
}

bool ScalarStatisticAggregationLogicalFunction::operator==(const ScalarStatisticAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction and statisticId == other.statisticId and op == other.op;
}

ScalarStatisticAggregationLogicalFunction
ScalarStatisticAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    const auto newInputFunction = inferFieldAccess(inputFunction, schema);

    /// Count tallies rows and does not care about the value's type; Sum and Avg have to arithmetic over it.
    if (op != StatisticType::Count and not newInputFunction->getDataType().isNumeric())
    {
        throw CannotInferStamp(
            "Cannot compute a {} statistic over a non-numeric function (got {})",
            magic_enum::enum_name(op),
            newInputFunction->getDataType());
    }
    return ScalarStatisticAggregationLogicalFunction{newInputFunction, statisticId, op};
}

Reflected Reflector<ScalarStatisticAggregationLogicalFunction>::operator()(
    const ScalarStatisticAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedScalarStatisticAggregationLogicalFunction{
        .inputFunction = function.getInputFunction(),
        .statisticId = function.getStatisticId().getRawValue(),
        .op = static_cast<uint64_t>(function.getOp())});
}

ScalarStatisticAggregationLogicalFunction
Unreflector<ScalarStatisticAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [inputFunction, statisticId, op]
        = context.unreflect<detail::ReflectedScalarStatisticAggregationLogicalFunction>(reflected);
    const auto decodedOp = magic_enum::enum_cast<StatisticType>(static_cast<uint8_t>(op));
    if (not decodedOp.has_value())
    {
        throw CannotDeserialize("Unknown scalar statistic op: {}", op);
    }
    return ScalarStatisticAggregationLogicalFunction{std::move(inputFunction), StatisticId{statisticId}, decodedOp.value()};
}

}

size_t std::hash<NES::ScalarStatisticAggregationLogicalFunction>::operator()(
    const NES::ScalarStatisticAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(
        aggregationFunction.getInputFunction(),
        NES::ScalarStatisticAggregationLogicalFunction::getName(),
        aggregationFunction.getStatisticId().getRawValue(),
        static_cast<uint64_t>(aggregationFunction.getOp()));
}
