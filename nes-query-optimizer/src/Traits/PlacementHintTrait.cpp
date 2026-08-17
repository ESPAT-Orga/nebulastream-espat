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

#include <Traits/PlacementHintTrait.hpp>

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>

#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <TraitRegisty.hpp>

namespace NES
{
/// Required for plugin registration, no implementation necessary
TraitRegistryReturnType
TraitGeneratedRegistrar::RegisterPlacementHintTrait(TraitRegistryArguments arguments) /// NOLINT(performance-unnecessary-value-param)
{
    return unreflect<PlacementHintTrait>(arguments.reflected);
}

PlacementHintTrait::PlacementHintTrait(PlacementAnchor anchor) : anchor(anchor)
{
}

const std::type_info& PlacementHintTrait::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return typeid(PlacementHintTrait);
}

size_t PlacementHintTrait::hash() const
{
    return std::hash<std::underlying_type_t<PlacementAnchor>>{}(magic_enum::enum_integer(anchor));
}

std::string PlacementHintTrait::explain(ExplainVerbosity) const
{
    return fmt::format("PlacementHintTrait: {}", magic_enum::enum_name(anchor));
}

std::string_view PlacementHintTrait::getName() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

Reflected Reflector<PlacementHintTrait>::operator()(const PlacementHintTrait& trait) const
{
    return reflect(detail::ReflectedPlacementHintTrait{trait.anchor});
}

PlacementHintTrait Unreflector<PlacementHintTrait>::operator()(const Reflected& reflected) const
{
    auto [anchor] = unreflect<detail::ReflectedPlacementHintTrait>(reflected);
    return PlacementHintTrait{anchor};
}

}
