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

#include <Traits/PinnedHostTrait.hpp>

#include <cstddef>
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

const std::type_info& PinnedHostTrait::getType() const
{
    return typeid(PinnedHostTrait);
}

size_t PinnedHostTrait::hash() const
{
    return std::hash<std::string>{}(host.getRawValue());
}

std::string PinnedHostTrait::explain(ExplainVerbosity) const
{
    return fmt::format("PinnedHostTrait: host={}", host.getRawValue());
}

std::string_view PinnedHostTrait::getName() const
{
    return NAME;
}

Reflected Reflector<PinnedHostTrait>::operator()(const PinnedHostTrait& trait, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedPinnedHostTrait{trait.host});
}

PinnedHostTrait Unreflector<PinnedHostTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [host] = context.unreflect<detail::ReflectedPinnedHostTrait>(reflected);
    return PinnedHostTrait{host};
}

}
