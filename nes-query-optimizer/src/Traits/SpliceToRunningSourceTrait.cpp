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

#include <Traits/SpliceToRunningSourceTrait.hpp>

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
TraitRegistryReturnType TraitGeneratedRegistrar::RegisterSpliceToRunningSourceTrait(
    TraitRegistryArguments arguments) /// NOLINT(performance-unnecessary-value-param)
{
    return unreflect<SpliceToRunningSourceTrait>(arguments.reflected);
}

const std::type_info& SpliceToRunningSourceTrait::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return typeid(SpliceToRunningSourceTrait);
}

size_t SpliceToRunningSourceTrait::hash() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return std::type_index(typeid(SpliceToRunningSourceTrait)).hash_code();
}

std::string SpliceToRunningSourceTrait::explain(ExplainVerbosity) const /// NOLINT(readability-convert-member-functions-to-static)
{
    return std::string{NAME};
}

std::string_view SpliceToRunningSourceTrait::getName() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

Reflected Reflector<SpliceToRunningSourceTrait>::operator()(const SpliceToRunningSourceTrait&) const
{
    return reflect(detail::ReflectedSpliceToRunningSourceTrait{});
}

SpliceToRunningSourceTrait Unreflector<SpliceToRunningSourceTrait>::operator()(const Reflected&) const
{
    return SpliceToRunningSourceTrait{};
}

}
