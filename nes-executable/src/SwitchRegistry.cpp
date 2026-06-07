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
#include <SwitchRegistry.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace NES
{

SwitchRegistry& SwitchRegistry::instance()
{
    static SwitchRegistry singleton;
    return singleton;
}

std::shared_ptr<std::atomic<int64_t>> SwitchRegistry::getOrCreate(const std::string& name, int64_t initial)
{
    std::lock_guard guard{mutex};
    auto& slot = switches[name];
    if (not slot)
    {
        slot = std::make_shared<std::atomic<int64_t>>(initial);
    }
    return slot;
}

std::shared_ptr<std::atomic<int64_t>> SwitchRegistry::tryGet(const std::string& name) const
{
    std::lock_guard guard{mutex};
    if (const auto it = switches.find(name); it != switches.end())
    {
        return it->second;
    }
    return nullptr;
}

void SwitchRegistry::set(const std::string& name, int64_t value)
{
    auto slot = getOrCreate(name, value);
    slot->store(value, std::memory_order_relaxed);
}

}
