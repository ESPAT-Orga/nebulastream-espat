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
#include <Util/Reflection.hpp>

namespace NES
{

/// Trait stamped on a logical source operator to instruct the runtime: register the RunningSource
/// in the RunningSourceRegistry but DO NOT start its emit thread until `expectedSpliceCount`
/// successful appendSuccessors() calls have happened.
///
/// Used by collectWorkloadStatistic so the data query's source doesn't start emitting until
/// ALL expected splice-mode build branches (SpliceToRunningSourceTrait) have grafted their head
/// pipelines onto the source. Without this gate, the source begins emitting at sequence 0
/// immediately on data-query deployment; by the time later build branches wire in (a few hundred
/// ms later) they receive a non-zero starting sequence number and their MultiOriginWatermark-
/// Processor gets stuck waiting for sequences 0..N-1 forever.
struct DeferSourceStartTrait final
{
    static constexpr std::string_view NAME = "DeferSourceStart";

    /// Number of successful appendSuccessors() calls that must happen before the source starts
    /// emitting. Default 1 (matches the single-splice case: one build branch attaches → source
    /// starts). For N build branches set this to N before submitting the data plan.
    uint32_t expectedSpliceCount = 1;

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const DeferSourceStartTrait&) const = default;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<DeferSourceStartTrait>;
};

template <>
struct Reflector<DeferSourceStartTrait>
{
    Reflected operator()(const DeferSourceStartTrait& trait) const;
};

template <>
struct Unreflector<DeferSourceStartTrait>
{
    DeferSourceStartTrait operator()(const Reflected& reflected) const;
};

static_assert(TraitConcept<DeferSourceStartTrait>);

}

namespace NES::detail
{
struct ReflectedDeferSourceStartTrait
{
    uint32_t expectedSpliceCount = 1;
};
}
