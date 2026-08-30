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

#include <Operators/Windows/StatisticBuildLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Statistic/StatisticTargetUtil.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/WindowAggregationLogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

StatisticBuildLogicalOperator::StatisticBuildLogicalOperator(
    WeakLogicalOperator self,
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
    Windowing::TimeBasedWindowType windowType,
    std::shared_ptr<LogicalStatisticFields> logicalStatisticFields)
    : ManagedByOperator(std::move(self))
    , aggregationFunctions(std::move(aggregationFunctions))
    , windowType(std::move(windowType))
    , logicalStatisticFields(std::move(logicalStatisticFields))
{
    PRECONDITION(this->logicalStatisticFields != nullptr, "A StatisticBuild operator always needs logicalStatisticFields.");
}

StatisticBuildLogicalOperator::StatisticBuildLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
    Windowing::TimeBasedWindowType windowType,
    std::shared_ptr<LogicalStatisticFields> logicalStatisticFields)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , aggregationFunctions(std::move(aggregationFunctions))
    , windowType(std::move(windowType))
    , logicalStatisticFields(std::move(logicalStatisticFields))
{
    PRECONDITION(this->logicalStatisticFields != nullptr, "A StatisticBuild operator always needs logicalStatisticFields.");
    inferLocalSchema();
}

TypedLogicalOperator<StatisticBuildLogicalOperator> StatisticBuildLogicalOperator::create(
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
    Windowing::TimeBasedWindowType windowType,
    std::shared_ptr<LogicalStatisticFields> logicalStatisticFields)
{
    return TypedLogicalOperator<StatisticBuildLogicalOperator>{
        std::move(aggregationFunctions), std::move(windowType), std::move(logicalStatisticFields)};
}

void StatisticBuildLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const Schema<Field, Unordered>& inputSchema = child->getOutputSchema();

    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> newFunctions;
    for (const auto& agg : aggregationFunctions)
    {
        newFunctions.emplace_back(std::make_shared<WindowAggregationLogicalFunction>(agg->withInferredType(inputSchema)));
    }
    aggregationFunctions = std::move(newFunctions);

    windowStartFieldName = logicalStatisticFields->statisticStartTsField.name;
    windowEndFieldName = logicalStatisticFields->statisticEndTsField.name;

    if (aggregationFunctions.empty())
    {
        throw CannotInferSchema("A StatisticBuild operator requires at least one statistic aggregation");
    }

    std::vector<UnqualifiedUnboundField> outputFields;
    outputFields.emplace_back(Identifier::parse(windowStartFieldName), logicalStatisticFields->statisticStartTsField.dataType);
    outputFields.emplace_back(Identifier::parse(windowEndFieldName), logicalStatisticFields->statisticEndTsField.dataType);

    std::unordered_set<StatisticTuple::StatisticId::Underlying> seenStatisticIds;
    const auto dataFieldType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE);
    for (const auto& aggregation : aggregationFunctions)
    {
        const auto target = tryGetStatisticTarget(*aggregation);
        if (!target.has_value())
        {
            throw CannotInferSchema("StatisticBuild expects only statistic aggregations but got {}", aggregation->getName());
        }
        if (!seenStatisticIds.insert(target->statisticId.getRawValue()).second)
        {
            throw CannotInferSchema(
                "StatisticBuild requires distinct statisticIds across its aggregations but {} appears more than once",
                target->statisticId.getRawValue());
        }
        outputFields.emplace_back(Identifier::parse(statisticDataFieldName(target->statisticId)), dataFieldType);
    }
    outputFields.emplace_back(
        Identifier::parse(logicalStatisticFields->statisticNumberOfSeenTuplesField.name),
        logicalStatisticFields->statisticNumberOfSeenTuplesField.dataType);

    const auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in statistic output schema: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    outputSchema = outputSchemaOrCollisions.value();
}

std::string_view StatisticBuildLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string StatisticBuildLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "STATISTIC BUILD(opId: {}, {}, window type: {})",
            id,
            fmt::join(std::views::transform(aggregationFunctions, [](const auto& agg) { return agg->getName(); }), ", "),
            windowType);
    }
    return fmt::format(
        "STAT BUILD({})", fmt::join(std::views::transform(aggregationFunctions, [](const auto& agg) { return agg->getName(); }), ", "));
}

bool StatisticBuildLogicalOperator::operator==(const StatisticBuildLogicalOperator& rhs) const
{
    if (aggregationFunctions.size() != rhs.aggregationFunctions.size())
    {
        return false;
    }
    for (uint64_t i = 0; i < aggregationFunctions.size(); i++)
    {
        if (*aggregationFunctions[i] != *rhs.aggregationFunctions[i])
        {
            return false;
        }
    }
    if (*logicalStatisticFields != *rhs.logicalStatisticFields)
    {
        return false;
    }
    return windowType == rhs.windowType && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticBuild, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticBuild, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> StatisticBuildLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Retrieving output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> StatisticBuildLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> StatisticBuildLogicalOperator::getWindowAggregation() const
{
    return aggregationFunctions;
}

void StatisticBuildLogicalOperator::setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> wa)
{
    aggregationFunctions = std::move(wa);
}

Windowing::TimeBasedWindowType StatisticBuildLogicalOperator::getWindowType() const
{
    return windowType;
}

void StatisticBuildLogicalOperator::setWindowType(Windowing::TimeBasedWindowType wt)
{
    windowType = std::move(wt);
}

std::vector<FieldAccessLogicalFunction> StatisticBuildLogicalOperator::getGroupingKeys() const
{
    return groupingKey;
}

std::string StatisticBuildLogicalOperator::getWindowStartFieldName() const
{
    return windowStartFieldName;
}

std::string StatisticBuildLogicalOperator::getWindowEndFieldName() const
{
    return windowEndFieldName;
}

UnqualifiedUnboundField StatisticBuildLogicalOperator::getWindowStartField() const
{
    return {Identifier::parse(windowStartFieldName), logicalStatisticFields->statisticStartTsField.dataType};
}

UnqualifiedUnboundField StatisticBuildLogicalOperator::getWindowEndField() const
{
    return {Identifier::parse(windowEndFieldName), logicalStatisticFields->statisticEndTsField.dataType};
}

std::string StatisticBuildLogicalOperator::getNumberOfSeenTuplesFieldName() const
{
    return logicalStatisticFields->statisticNumberOfSeenTuplesField.name;
}

TraitSet StatisticBuildLogicalOperator::getTraitSet() const
{
    return traitSet;
}

Reflected Reflector<StatisticBuildLogicalOperator>::operator()(const StatisticBuildLogicalOperator& op, const ReflectionContext& context) const
{
    std::vector<std::pair<std::string, Reflected>> windowAggregations;
    for (const auto& agg : op.getWindowAggregation())
    {
        windowAggregations.emplace_back(agg->getName(), agg->reflect(context));
    }
    return context.reflect(detail::ReflectedStatisticBuildLogicalOperator{
        .aggregations = std::move(windowAggregations), .windowType = context.reflect(op.getWindowType())});
}

StatisticBuildLogicalOperator Unreflector<StatisticBuildLogicalOperator>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [aggregations, windowTypeReflected] = context.unreflect<detail::ReflectedStatisticBuildLogicalOperator>(reflected);
    auto windowType = context.unreflect<Windowing::TimeBasedWindowType>(windowTypeReflected);

    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions;
    for (const auto& [name, reflectedAggregation] : aggregations)
    {
        auto function = context.unreflect<WindowAggregationLogicalFunction>(reflectedAggregation);
        aggregationFunctions.emplace_back(std::make_shared<WindowAggregationLogicalFunction>(std::move(function)));
    }

    return StatisticBuildLogicalOperator{WeakLogicalOperator{}, std::move(aggregationFunctions), std::move(windowType), std::make_shared<LogicalStatisticFields>()};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterStatisticBuildLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<StatisticBuildLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Expected arguments are missing");
    std::unreachable();
}

}

std::size_t std::hash<NES::StatisticBuildLogicalOperator>::operator()(const NES::StatisticBuildLogicalOperator& op) const noexcept
{
    std::size_t seed = folly::hash::hash_combine_generic(NES::Hash{}, op.windowType);
    for (const auto& agg : op.aggregationFunctions)
    {
        seed = folly::hash::hash_combine(seed, std::hash<NES::WindowAggregationLogicalFunction>{}(*agg));
    }
    return seed;
}
