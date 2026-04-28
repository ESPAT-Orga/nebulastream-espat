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

#include <Functions/H3HashLogicalFunction.hpp>

#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

H3HashLogicalFunction::H3HashLogicalFunction(
    const LogicalFunction& value,
    const LogicalFunction& rowIndex,
    const LogicalFunction& numberOfRows,
    const LogicalFunction& numberOfCols,
    const LogicalFunction& seed)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UINT64))
    , value(value)
    , rowIndex(rowIndex)
    , numberOfRows(numberOfRows)
    , numberOfCols(numberOfCols)
    , seed(seed)
{
}

bool H3HashLogicalFunction::operator==(const H3HashLogicalFunction& rhs) const
{
    return value == rhs.value and rowIndex == rhs.rowIndex and numberOfRows == rhs.numberOfRows and numberOfCols == rhs.numberOfCols
        and seed == rhs.seed;
}

std::string H3HashLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format(
        "H3_HASH({}, {}, {}, {}, {})",
        value.explain(verbosity),
        rowIndex.explain(verbosity),
        numberOfRows.explain(verbosity),
        numberOfCols.explain(verbosity),
        seed.explain(verbosity));
}

DataType H3HashLogicalFunction::getDataType() const
{
    return dataType;
}

H3HashLogicalFunction H3HashLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction H3HashLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 5, "H3HashLogicalFunction expects exactly five children but has {}", newChildren.size());
    /// The result is always a UINT64 (a column index modulo numberOfCols).
    return withDataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)).withChildren(newChildren);
}

std::vector<LogicalFunction> H3HashLogicalFunction::getChildren() const
{
    return {value, rowIndex, numberOfRows, numberOfCols, seed};
}

H3HashLogicalFunction H3HashLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    INVARIANT(children.size() == 5, "H3_HASH requires exactly 5 children, got {}", children.size());
    auto copy = *this;
    copy.value = children[0];
    copy.rowIndex = children[1];
    copy.numberOfRows = children[2];
    copy.numberOfCols = children[3];
    copy.seed = children[4];
    return copy;
}

std::string_view H3HashLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<H3HashLogicalFunction>::operator()(const H3HashLogicalFunction& function) const
{
    return reflect(detail::ReflectedH3HashLogicalFunction{
        .value = function.value,
        .rowIndex = function.rowIndex,
        .numberOfRows = function.numberOfRows,
        .numberOfCols = function.numberOfCols,
        .seed = function.seed});
}

H3HashLogicalFunction Unreflector<H3HashLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [value, rowIndex, numberOfRows, numberOfCols, seed] = unreflect<detail::ReflectedH3HashLogicalFunction>(reflected);
    if (!value.has_value() or !rowIndex.has_value() or !numberOfRows.has_value() or !numberOfCols.has_value() or !seed.has_value())
    {
        throw CannotDeserialize("Missing child function for H3_HASH");
    }
    return H3HashLogicalFunction{value.value(), rowIndex.value(), numberOfRows.value(), numberOfCols.value(), seed.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterH3_HASHLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<H3HashLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 5)
    {
        throw CannotDeserialize("H3_HASH requires exactly 5 children, got {}", arguments.children.size());
    }
    return H3HashLogicalFunction(
        arguments.children[0], arguments.children[1], arguments.children[2], arguments.children[3], arguments.children[4]);
}

}
