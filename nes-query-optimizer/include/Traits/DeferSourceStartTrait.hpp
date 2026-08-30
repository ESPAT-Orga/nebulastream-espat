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
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// Stamps a source operator so the runtime defers emission until all expectedSpliceCount
/// companion splice queries have connected their input pipelines. Set by StatisticManager
/// when deploying multi-splice statistic plans so all build branches start simultaneously.
struct DeferSourceStartTrait final
{
    static constexpr std::string_view NAME = "DeferSourceStart";
    uint32_t expectedSpliceCount{1};

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const DeferSourceStartTrait& other) const = default;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<DeferSourceStartTrait>;
};

template <>
struct Reflector<DeferSourceStartTrait>
{
    Reflected operator()(const DeferSourceStartTrait& trait, const ReflectionContext& context) const;
};

template <>
struct Unreflector<DeferSourceStartTrait>
{
    DeferSourceStartTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<DeferSourceStartTrait>);

}

namespace NES::detail
{
struct ReflectedDeferSourceStartTrait
{
    uint32_t expectedSpliceCount;
};
}
