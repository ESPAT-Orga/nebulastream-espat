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

#include <RunningSourceRegistry.hpp>

#include <memory>
#include <string>
#include <utility>

#include <ErrorHandling.hpp>
#include <RunningSource.hpp>

namespace NES
{

RunningSourceRegistry& RunningSourceRegistry::instance()
{
    static RunningSourceRegistry singleton;
    return singleton;
}

void RunningSourceRegistry::registerSource(const std::string& logicalSourceName, std::weak_ptr<RunningSource> source)
{
    auto locked = entries.wlock();
    if (auto it = locked->find(logicalSourceName); it != locked->end())
    {
        if (auto existing = it->second.lock())
        {
            if (existing == source.lock())
            {
                /// Idempotent re-registration of the same RunningSource.
                return;
            }
            /// A different live RunningSource is already registered. The splice contract assumes a
            /// single source thread per logical name, so we strictly refuse a second registration.
            throw InvalidConfigParameter(
                "RunningSourceRegistry: logical source '{}' already has a live registered source; "
                "cannot register a second source for the same logical name.",
                logicalSourceName);
        }
        /// Stale (expired) entry: overwrite below.
    }
    (*locked)[logicalSourceName] = std::move(source);
}

void RunningSourceRegistry::deregisterSource(const std::string& logicalSourceName)
{
    auto locked = entries.wlock();
    if (auto it = locked->find(logicalSourceName); it != locked->end())
    {
        /// Only erase if the slot is empty/expired. The destructor of a `RunningSource` that
        /// FAILED to register (e.g. because a duplicate already held the slot) must NOT remove
        /// the legitimate owner's entry. The legitimate owner deregisters via this same call
        /// only when its weak_ptr is the last one alive, by which point it has already expired.
        if (it->second.expired())
        {
            locked->erase(it);
        }
    }
}

std::shared_ptr<RunningSource> RunningSourceRegistry::tryLookup(const std::string& logicalSourceName) const
{
    auto locked = entries.rlock();
    if (auto it = locked->find(logicalSourceName); it != locked->end())
    {
        return it->second.lock();
    }
    return nullptr;
}

bool RunningSourceRegistry::startDeferred(const std::string& logicalSourceName)
{
    auto source = tryLookup(logicalSourceName);
    if (not source)
    {
        return false;
    }
    source->startEmitting();
    return true;
}

}
