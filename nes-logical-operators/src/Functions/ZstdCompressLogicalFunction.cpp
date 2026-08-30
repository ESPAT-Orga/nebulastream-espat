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
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp> /// NOLINT(misc-include-cleaner)
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h> /// NOLINT(misc-include-cleaner)

namespace NES
{

/// NOLINTNEXTLINE(modernize-pass-by-value)
ZstdCompressLogicalFunction::ZstdCompressLogicalFunction(const LogicalFunction& child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), child(child)
{
}

bool ZstdCompressLogicalFunction::operator==(const ZstdCompressLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string ZstdCompressLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("ZSTD_COMPRESS({})", child.explain(verbosity));
}

DataType ZstdCompressLogicalFunction::getDataType() const
{
    return dataType;
}

ZstdCompressLogicalFunction ZstdCompressLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction ZstdCompressLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (const auto& chr : getChildren())
    {
        newChildren.push_back(chr.withInferredDataType(schema));
    }
    INVARIANT(newChildren.size() == 1, "ZstdCompressLogicalFunction expects exactly one child but has {}", newChildren.size());
    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    newDataType.nullable = newChildren[0].getDataType().nullable;
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> ZstdCompressLogicalFunction::getChildren() const
{
    return {child};
}

ZstdCompressLogicalFunction ZstdCompressLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::string_view ZstdCompressLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<ZstdCompressLogicalFunction>::operator()(const ZstdCompressLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedZstdCompressLogicalFunction{.child = function.child});
}

ZstdCompressLogicalFunction Unreflector<ZstdCompressLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [child] = context.unreflect<detail::ReflectedZstdCompressLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("ZstdCompressLogicalFunction is missing its child");
    }
    return ZstdCompressLogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType ZstdCompressLogicalFunction::createZSTD_COMPRESS(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.empty())
    {
        throw CannotDeserialize("ZSTD_COMPRESS requires one argument");
    }
    return ZstdCompressLogicalFunction(arguments.children.back());
}

}
