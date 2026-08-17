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
#include <cstdint>
#include <string>
#include <string_view>
#include <typeinfo>

#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Where a placement hint pins its operator, expressed relative to the plan's endpoints so the
/// hint carries no knowledge of the concrete topology: `Source` pins to the node hosting the
/// operator's descendant source, `Sink` pins to the node hosting the root sink.
enum class PlacementAnchor : uint8_t
{
    Source,
    Sink
};

/// Generic placement hint: instructs `BottomUpOperatorPlacer` to hard-pin the tagged operator to
/// the node of the referenced endpoint (see `PlacementAnchor`). This decouples placement from any
/// particular feature — the placer only honours the hint and needs no feature-specific knowledge.
///
/// Used by the histogram delta-compression build chain (see
/// docs/histogram-delta-wire-compression-plan.md): the GEN StatisticBuild is anchored to `Source`
/// and the RESOLVER StatisticBuild + StatisticStoreWriter to `Sink`, which forces the two halves
/// onto different nodes so the optimizer inserts a network channel — and thus the delta-over-wire
/// cut — between them instead of collapsing them onto one node.
///
/// `PinnedHostTrait` pins an operator the same way but to an absolute host; it could be merged into this trait.
struct PlacementHintTrait final
{
    static constexpr std::string_view NAME = "PlacementHint";
    PlacementAnchor anchor;

    explicit PlacementHintTrait(PlacementAnchor anchor);

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
    Reflected operator()(const PlacementHintTrait& trait) const;
};

template <>
struct Unreflector<PlacementHintTrait>
{
    PlacementHintTrait operator()(const Reflected& reflected) const;
};

static_assert(TraitConcept<PlacementHintTrait>);

}

namespace NES::detail
{
struct ReflectedPlacementHintTrait
{
    PlacementAnchor anchor;
};
}
