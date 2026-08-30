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

#include <cstddef>
#include <string>
#include <string_view>
#include <typeinfo>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// Hints to the placer whether an operator should be co-located with the data source or sink.
enum class PlacementAnchor : uint8_t
{
    Source,
    Sink,
};

struct PlacementHintTrait final
{
    static constexpr std::string_view NAME = "PlacementHint";
    PlacementAnchor anchor{PlacementAnchor::Source};

    explicit PlacementHintTrait(PlacementAnchor a) : anchor(a) { }

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const PlacementHintTrait& other) const = default;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<PlacementHintTrait>;
};

template <>
struct Reflector<PlacementHintTrait>
{
    Reflected operator()(const PlacementHintTrait& trait, const ReflectionContext& context) const;
};

template <>
struct Unreflector<PlacementHintTrait>
{
    PlacementHintTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<PlacementHintTrait>);

}

namespace NES::detail
{
struct ReflectedPlacementHintTrait
{
    uint8_t anchor{};
};
}
