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

#include <Traits/DeferSourceStartTrait.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <typeinfo>

#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

const std::type_info& DeferSourceStartTrait::getType() const
{
    return typeid(DeferSourceStartTrait);
}

size_t DeferSourceStartTrait::hash() const
{
    return std::hash<uint32_t>{}(expectedSpliceCount);
}

std::string DeferSourceStartTrait::explain(ExplainVerbosity) const
{
    return fmt::format("DeferSourceStartTrait: expectedSpliceCount={}", expectedSpliceCount);
}

std::string_view DeferSourceStartTrait::getName() const
{
    return NAME;
}

Reflected Reflector<DeferSourceStartTrait>::operator()(const DeferSourceStartTrait& trait, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedDeferSourceStartTrait{trait.expectedSpliceCount});
}

DeferSourceStartTrait Unreflector<DeferSourceStartTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [expectedSpliceCount] = context.unreflect<detail::ReflectedDeferSourceStartTrait>(reflected);
    return DeferSourceStartTrait{.expectedSpliceCount = expectedSpliceCount};
}

}
