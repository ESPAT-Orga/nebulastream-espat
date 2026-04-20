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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Windowing/WindowMetaData.hpp>

namespace NES
{

class StatisticBuildLogicalOperator : public OriginIdAssigner
{
public:
    StatisticBuildLogicalOperator(
        std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
        std::shared_ptr<Windowing::WindowType> windowType,
        std::shared_ptr<LogicalStatisticFields> logicalStatisticFields);

    [[nodiscard]] std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> getWindowAggregation() const;
    void setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregation);

    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    void setWindowType(std::shared_ptr<Windowing::WindowType> windowType);

    [[nodiscard]] std::vector<FieldAccessLogicalFunction> getGroupingKeys() const;

    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;
    [[nodiscard]] const WindowMetaData& getWindowMetaData() const;

    [[nodiscard]] std::string getNumberOfSeenTuplesFieldName() const;


    [[nodiscard]] bool operator==(const StatisticBuildLogicalOperator& rhs) const;

    [[nodiscard]] StatisticBuildLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] StatisticBuildLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] StatisticBuildLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    static constexpr std::string_view NAME = "StatisticBuild";

    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions;
    std::shared_ptr<Windowing::WindowType> windowType;
    std::vector<FieldAccessLogicalFunction> groupingKey;
    WindowMetaData windowMetaData;

    std::shared_ptr<LogicalStatisticFields> logicalStatisticFields;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
};

template <>
struct Reflector<StatisticBuildLogicalOperator>
{
    Reflected operator()(const StatisticBuildLogicalOperator&) const;
};

template <>
struct Unreflector<StatisticBuildLogicalOperator>
{
    StatisticBuildLogicalOperator operator()(const Reflected&) const;
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
