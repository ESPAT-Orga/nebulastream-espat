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

#include <Functions/ZstdCompressLogicalFunction.hpp>

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

/// the logical function sets the type to the type of its child
/// in practice, the physical function will always return a VARSIZED containing the compressed data
/// we set the "wrong" type because the decompression operator needs to know the decompressed type to emit
/// this discrepancy between types on the logical and physical level could present a problem in the future
ZstdCompressLogicalFunction::ZstdCompressLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child)
{
}

bool ZstdCompressLogicalFunction::operator==(const ZstdCompressLogicalFunction& rhs) const
{
    return child == rhs.child && compressionLevel == rhs.compressionLevel;
}

std::string ZstdCompressLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("ZSTD_COMPRESS({}, {})", child.explain(verbosity), static_cast<uint8_t>(compressionLevel));
}

DataType ZstdCompressLogicalFunction::getDataType() const
{
    return dataType;
};

ZstdCompressLogicalFunction ZstdCompressLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction ZstdCompressLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> ZstdCompressLogicalFunction::getChildren() const
{
    return {child};
};

ZstdCompressLogicalFunction ZstdCompressLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0];
    copy.dataType = children[0].getDataType();
    return copy;
};

std::string_view ZstdCompressLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<ZstdCompressLogicalFunction>::operator()(const ZstdCompressLogicalFunction& function) const
{
    return reflect(detail::ReflectedZstdCompressLogicalFunction{
        .child = function.child, .compressionLevel = static_cast<uint8_t>(function.compressionLevel)});
}

ZstdCompressLogicalFunction Unreflector<ZstdCompressLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child, compressionLevel] = unreflect<detail::ReflectedZstdCompressLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("Missing child function");
    }
    return ZstdCompressLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterZstdCompressLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ZstdCompressLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("ZstdCompressLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return ZstdCompressLogicalFunction(arguments.children[0]);
}

uint8_t ZstdCompressLogicalFunction::getZstdCompressionLevel() const
{
    return static_cast<uint8_t>(compressionLevel);
}
}
