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

/// Marker trait stamped on a logical source operator to instruct the worker:
/// at instantiation time, do NOT spawn a new source thread. Instead, look up
/// an already-running source pipeline that serves the same logical source and
/// append this query's source-successors to it. Strict: if no matching running
/// source exists, fail. Used by the workload-domain statistic build branch to
/// share the data query's single source thread.
struct SpliceToRunningSourceTrait final
{
    static constexpr std::string_view NAME = "SpliceToRunningSource";

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const SpliceToRunningSourceTrait&) const = default;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<SpliceToRunningSourceTrait>;
};

template <>
struct Reflector<SpliceToRunningSourceTrait>
{
    Reflected operator()(const SpliceToRunningSourceTrait& trait) const;
};

template <>
struct Unreflector<SpliceToRunningSourceTrait>
{
    SpliceToRunningSourceTrait operator()(const Reflected& reflected) const;
};

static_assert(TraitConcept<SpliceToRunningSourceTrait>);

}

namespace NES::detail
{
struct ReflectedSpliceToRunningSourceTrait
{
};
}
