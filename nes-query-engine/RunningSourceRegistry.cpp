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
#include <vector>

#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <RunningQueryPlan.hpp>
#include <RunningSource.hpp>

namespace NES
{

RunningSourceRegistry& RunningSourceRegistry::instance()
{
    static RunningSourceRegistry singleton;
    return singleton;
}

bool RunningSourceRegistry::spliceOrEnqueue(
    const std::string& logicalSourceName, std::vector<std::shared_ptr<RunningQueryPlanNode>>&& successorNodes)
{
    /// Capture a strong ref to the target under the lock (if registered), then graft OUTSIDE the
    /// lock: appendSuccessors may fire the deferred start (source->start) under the source's own
    /// mutex, which must never run while holding the registry lock.
    std::shared_ptr<RunningSource> target;
    {
        auto locked = state.wlock();
        if (auto it = locked->entries.find(logicalSourceName); it != locked->entries.end())
        {
            target = it->second.lock();
        }
        if (not target)
        {
            /// No live source yet (absent or expired): queue the graft for registerSourceAndDrain.
            locked->pendingSplices[logicalSourceName].push_back(std::move(successorNodes));
            ENGINE_LOG_WARNING(
                "RunningSourceRegistry: splice for logical source '{}' queued; waiting for the data query to register it.",
                logicalSourceName);
            return false;
        }
    }
    target->appendSuccessors(std::move(successorNodes));
    return true;
}

void RunningSourceRegistry::registerSourceAndDrain(const std::string& logicalSourceName, const std::shared_ptr<RunningSource>& source)
{
    /// Insert the entry and extract any pre-registration splice batches under the lock; graft them
    /// OUTSIDE the lock (appendSuccessors may start the source, see spliceOrEnqueue).
    std::vector<std::vector<std::shared_ptr<RunningQueryPlanNode>>> queued;
    {
        auto locked = state.wlock();
        if (auto it = locked->entries.find(logicalSourceName); it != locked->entries.end())
        {
            if (auto existing = it->second.lock())
            {
                if (existing != source)
                {
                    /// A different live RunningSource is already registered. The splice contract assumes
                    /// a single source thread per logical name, so we strictly refuse a second one.
                    throw InvalidConfigParameter(
                        "RunningSourceRegistry: logical source '{}' already has a live registered source; "
                        "cannot register a second source for the same logical name.",
                        logicalSourceName);
                }
                /// Idempotent re-registration of the same RunningSource: nothing to drain.
                return;
            }
            /// Stale (expired) entry: overwrite below.
        }
        locked->entries[logicalSourceName] = std::weak_ptr<RunningSource>(source);
        if (auto pendingIt = locked->pendingSplices.find(logicalSourceName); pendingIt != locked->pendingSplices.end())
        {
            queued = std::move(pendingIt->second);
            locked->pendingSplices.erase(pendingIt);
        }
    }
    for (auto& batch : queued)
    {
        /// Each drained batch counts down the source's pendingSplices budget; the last one fires
        /// the deferred start (RunningSource::appendSuccessors).
        source->appendSuccessors(std::move(batch));
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
            /// The owning source is gone, so any splices still queued for this name can never be
            /// grafted onto it — reap them. Dropping the node shared_ptrs lets the build branch
            /// terminate cleanly instead of leaking until process exit.
            locked->pendingSplices.erase(logicalSourceName);
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

}
