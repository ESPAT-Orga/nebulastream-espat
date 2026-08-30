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
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <typeinfo>

#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>

namespace NES
{

const std::type_info& PlacementHintTrait::getType() const
{
    return typeid(PlacementHintTrait);
}

size_t PlacementHintTrait::hash() const
{
    return std::hash<uint8_t>{}(static_cast<uint8_t>(anchor));
}

std::string PlacementHintTrait::explain(ExplainVerbosity) const
{
    return fmt::format("PlacementHintTrait: anchor={}", anchor == PlacementAnchor::Source ? "Source" : "Sink");
}

std::string_view PlacementHintTrait::getName() const
{
    return NAME;
}

Reflected Reflector<PlacementHintTrait>::operator()(const PlacementHintTrait& trait, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedPlacementHintTrait{static_cast<uint8_t>(trait.anchor)});
}

PlacementHintTrait Unreflector<PlacementHintTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [anchor] = context.unreflect<detail::ReflectedPlacementHintTrait>(reflected);
    return PlacementHintTrait{static_cast<PlacementAnchor>(anchor)};
}

}
