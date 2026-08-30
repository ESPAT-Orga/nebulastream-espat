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
#include <typeinfo>

#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

const std::type_info& SpliceToRunningSourceTrait::getType() const
{
    return typeid(SpliceToRunningSourceTrait);
}

size_t SpliceToRunningSourceTrait::hash() const
{
    return 0;
}

std::string SpliceToRunningSourceTrait::explain(ExplainVerbosity) const
{
    return "SpliceToRunningSourceTrait";
}

std::string_view SpliceToRunningSourceTrait::getName() const
{
    return NAME;
}

Reflected Reflector<SpliceToRunningSourceTrait>::operator()(const SpliceToRunningSourceTrait&, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedSpliceToRunningSourceTrait{});
}

SpliceToRunningSourceTrait Unreflector<SpliceToRunningSourceTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    context.unreflect<detail::ReflectedSpliceToRunningSourceTrait>(reflected);
    return SpliceToRunningSourceTrait{};
}

}
