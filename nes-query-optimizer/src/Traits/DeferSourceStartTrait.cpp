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

#include <Traits/DeferSourceStartTrait.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <typeindex>
#include <typeinfo>

#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <TraitRegisty.hpp>

namespace NES
{
TraitRegistryReturnType
TraitGeneratedRegistrar::RegisterDeferSourceStartTrait(TraitRegistryArguments arguments) /// NOLINT(performance-unnecessary-value-param)
{
    return unreflect<DeferSourceStartTrait>(arguments.reflected);
}

const std::type_info& DeferSourceStartTrait::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return typeid(DeferSourceStartTrait);
}

size_t DeferSourceStartTrait::hash() const
{
    return std::type_index(typeid(DeferSourceStartTrait)).hash_code() ^ std::hash<uint32_t>{}(expectedSpliceCount);
}

std::string DeferSourceStartTrait::explain(ExplainVerbosity) const
{
    return std::string{NAME} + "(expectedSpliceCount=" + std::to_string(expectedSpliceCount) + ")";
}

std::string_view DeferSourceStartTrait::getName() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

Reflected Reflector<DeferSourceStartTrait>::operator()(const DeferSourceStartTrait& trait) const
{
    return reflect(detail::ReflectedDeferSourceStartTrait{trait.expectedSpliceCount});
}

DeferSourceStartTrait Unreflector<DeferSourceStartTrait>::operator()(const Reflected& reflected) const
{
    auto [count] = unreflect<detail::ReflectedDeferSourceStartTrait>(reflected);
    return DeferSourceStartTrait{.expectedSpliceCount = count};
}

}
