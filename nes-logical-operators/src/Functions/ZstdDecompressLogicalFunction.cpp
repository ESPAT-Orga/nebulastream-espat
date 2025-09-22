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

#include <Functions/ZstdDecompressLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
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

/// the type of the child operator is only used to determine the type of the output
/// the physical function always expects a VARSIZED containing the compressed data as input regardless of the logical type of the child function
ZstdDecompressLogicalFunction::ZstdDecompressLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child)
{
}

bool ZstdDecompressLogicalFunction::operator==(const ZstdDecompressLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string ZstdDecompressLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("ZSTD_DECOMPRESS({})", child.explain(verbosity));
}

DataType ZstdDecompressLogicalFunction::getDataType() const
{
    return dataType;
};

ZstdDecompressLogicalFunction ZstdDecompressLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction ZstdDecompressLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> ZstdDecompressLogicalFunction::getChildren() const
{
    return {child};
};

ZstdDecompressLogicalFunction ZstdDecompressLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0];
    copy.dataType = children[0].getDataType();
    return copy;
};

std::string_view ZstdDecompressLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<ZstdDecompressLogicalFunction>::operator()(const ZstdDecompressLogicalFunction& function) const
{
    return reflect(detail::ReflectedZstdDecompressLogicalFunction{.child = function.child});
}

ZstdDecompressLogicalFunction Unreflector<ZstdDecompressLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedZstdDecompressLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("Missing child function");
    }
    return ZstdDecompressLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterZstdDecompressLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ZstdDecompressLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("ZstdDecompressLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return ZstdDecompressLogicalFunction(arguments.children[0]);
}

}
