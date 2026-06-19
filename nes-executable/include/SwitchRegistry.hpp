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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace NES
{

/// Holds a single named atomic int64 used as a runtime gate for pipeline stages.
/// Pipeline stages compare the atomic's value against their expected value once per
/// buffer to decide whether to run. External code (e.g. an adaptive-swap callback)
/// flips the value to redirect buffer flow between sibling filter chains without
/// stopping and redeploying the query.
class SwitchRegistry
{
public:
    static SwitchRegistry& instance();

    /// Returns the shared atomic for `name`, creating it (initialized to `initial`)
    /// if it doesn't exist yet. Subsequent calls with the same name return the
    /// same atomic regardless of `initial`.
    std::shared_ptr<std::atomic<int64_t>> getOrCreate(const std::string& name, int64_t initial = 0);

    /// Returns the atomic for `name` if it exists, otherwise nullptr.
    std::shared_ptr<std::atomic<int64_t>> tryGet(const std::string& name) const;

    /// Stores `value` into the atomic registered as `name`, creating the slot
    /// (with the given value as its initial state) if it doesn't exist.
    void set(const std::string& name, int64_t value);

private:
    SwitchRegistry() = default;
    mutable std::mutex mutex;
    std::unordered_map<std::string, std::shared_ptr<std::atomic<int64_t>>> switches;
};

}
