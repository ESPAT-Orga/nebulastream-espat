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
#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// Pins an operator to a specific worker host so the placer does not move it.
struct PinnedHostTrait final
{
    static constexpr std::string_view NAME = "PinnedHost";
    Host host;

    explicit PinnedHostTrait(Host h) : host(std::move(h)) { }

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const PinnedHostTrait& other) const = default;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<PinnedHostTrait>;
};

template <>
struct Reflector<PinnedHostTrait>
{
    Reflected operator()(const PinnedHostTrait& trait, const ReflectionContext& context) const;
};

template <>
struct Unreflector<PinnedHostTrait>
{
    PinnedHostTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<PinnedHostTrait>);

}

namespace NES::detail
{
struct ReflectedPinnedHostTrait
{
    Host host;
};
}
