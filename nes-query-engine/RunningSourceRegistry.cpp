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

#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

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
    /// Mutate the maps under the lock; run appendSuccessors (which may startEmitting → spawn the
    /// source thread) afterwards, outside the lock.
    std::shared_ptr<RunningSource> live;
    std::vector<std::shared_ptr<RunningQueryPlanNode>> drained;
    {
        auto locked = state.wlock();
        if (auto it = locked->entries.find(logicalSourceName); it != locked->entries.end())
        {
            if (auto existing = it->second.lock())
            {
                if (existing == source.lock())
                {
                    /// Idempotent re-registration of the same RunningSource.
                    return;
                }
                /// A different live RunningSource is already registered. The splice contract assumes
                /// a single source thread per logical name, so we strictly refuse a second one.
                throw InvalidConfigParameter(
                    "RunningSourceRegistry: logical source '{}' already has a live registered source; "
                    "cannot register a second source for the same logical name.",
                    logicalSourceName);
            }
            /// Stale (expired) entry: overwrite below.
        }
        locked->entries[logicalSourceName] = source;

        /// Drain any splices that raced ahead of this source's registration so they can be grafted
        /// on now. The caller has already set the deferred-start budget, so these count correctly.
        if (auto pit = locked->pendingSplices.find(logicalSourceName); pit != locked->pendingSplices.end())
        {
            drained = std::move(pit->second);
            locked->pendingSplices.erase(pit);
            live = source.lock();
        }
    }
    if (live && not drained.empty())
    {
        live->appendSuccessors(std::move(drained));
    }
}

void RunningSourceRegistry::deregisterSource(const std::string& logicalSourceName)
{
    auto locked = state.wlock();
    if (auto it = locked->entries.find(logicalSourceName); it != locked->entries.end())
    {
        /// Only erase if the slot is empty/expired. The destructor of a `RunningSource` that
        /// FAILED to register (e.g. because a duplicate already held the slot) must NOT remove
        /// the legitimate owner's entry. The legitimate owner deregisters via this same call
        /// only when its weak_ptr is the last one alive, by which point it has already expired.
        if (it->second.expired())
        {
            locked->entries.erase(it);
        }
    }
}

std::shared_ptr<RunningSource> RunningSourceRegistry::tryLookup(const std::string& logicalSourceName) const
{
    auto locked = state.rlock();
    if (auto it = locked->entries.find(logicalSourceName); it != locked->entries.end())
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

void RunningSourceRegistry::spliceOrDefer(
    const std::string& logicalSourceName, std::vector<std::shared_ptr<RunningQueryPlanNode>> successors)
{
    /// Decide live-vs-pending atomically under the lock; if live, capture the shared_ptr and run
    /// appendSuccessors after releasing the lock (it may startEmitting). If not live yet, stash the
    /// successors so registerSource grafts them on when the source appears.
    std::shared_ptr<RunningSource> live;
    {
        auto locked = state.wlock();
        if (auto it = locked->entries.find(logicalSourceName); it != locked->entries.end())
        {
            live = it->second.lock();
        }
        if (not live)
        {
            auto& pending = locked->pendingSplices[logicalSourceName];
            pending.insert(
                pending.end(), std::make_move_iterator(successors.begin()), std::make_move_iterator(successors.end()));
            return;
        }
    }
    live->appendSuccessors(std::move(successors));
}

}
