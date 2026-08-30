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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

class StatisticBuildLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    StatisticBuildLogicalOperator(
        WeakLogicalOperator self,
        std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
        Windowing::TimeBasedWindowType windowType,
        std::shared_ptr<LogicalStatisticFields> logicalStatisticFields);

    StatisticBuildLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
        Windowing::TimeBasedWindowType windowType,
        std::shared_ptr<LogicalStatisticFields> logicalStatisticFields);

    static TypedLogicalOperator<StatisticBuildLogicalOperator> create(
        std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
        Windowing::TimeBasedWindowType windowType,
        std::shared_ptr<LogicalStatisticFields> logicalStatisticFields);

    [[nodiscard]] std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> getWindowAggregation() const;
    void setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregation);

    [[nodiscard]] Windowing::TimeBasedWindowType getWindowType() const;
    void setWindowType(Windowing::TimeBasedWindowType windowType);

    [[nodiscard]] std::vector<FieldAccessLogicalFunction> getGroupingKeys() const;

    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;
    [[nodiscard]] UnqualifiedUnboundField getWindowStartField() const;
    [[nodiscard]] UnqualifiedUnboundField getWindowEndField() const;

    [[nodiscard]] std::string getNumberOfSeenTuplesFieldName() const;

    [[nodiscard]] bool operator==(const StatisticBuildLogicalOperator& rhs) const;

    [[nodiscard]] StatisticBuildLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] StatisticBuildLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] StatisticBuildLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] StatisticBuildLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "StatisticBuild";

    std::optional<LogicalOperator> child;
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions;
    Windowing::TimeBasedWindowType windowType;
    std::vector<FieldAccessLogicalFunction> groupingKey;

    std::shared_ptr<LogicalStatisticFields> logicalStatisticFields;

    void inferLocalSchema();
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    /// Field names set during schema inference (qualified once the qualifier is known)
    std::string windowStartFieldName;
    std::string windowEndFieldName;

    TraitSet traitSet;

    friend struct std::hash<StatisticBuildLogicalOperator>;
    friend struct Reflector<StatisticBuildLogicalOperator>;
};

template <>
struct Reflector<StatisticBuildLogicalOperator>
{
    Reflected operator()(const StatisticBuildLogicalOperator& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<StatisticBuildLogicalOperator>
{
    StatisticBuildLogicalOperator operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<StatisticBuildLogicalOperator>);

namespace detail
{
struct ReflectedStatisticBuildLogicalOperator
{
    std::vector<std::pair<std::string, Reflected>> aggregations;
    Reflected windowType;
};
}

}

template <>
struct std::hash<NES::StatisticBuildLogicalOperator>
{
    uint64_t operator()(const NES::StatisticBuildLogicalOperator& op) const noexcept;
};
