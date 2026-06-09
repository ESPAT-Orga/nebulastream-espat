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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <folly/Synchronized.h>

namespace NES
{
class RunningSource;
struct RunningQueryPlanNode;

/// Worker-wide registry mapping logical source name -> live RunningSource. Populated by the
/// RunningQueryPlan setup callback after a non-splice source is created, cleared via
/// RunningSource's destructor. Used by the SpliceToRunningSourceTrait code path: when a query
/// arrives with that trait on its source, it grafts its head-pipeline nodes onto the running
/// source for the same logical name instead of spawning a fresh source thread.
///
/// Splice-before-register: the build branch and the data query are deployed as separate queries,
/// asynchronously, so a lightweight build branch can reach its splice before the data query has
/// registered its source. To avoid losing that splice, spliceOrDefer stashes the successors in a
/// per-name pending list and registerSource drains them when the source appears. The pending map
/// and the live-entry map live under one lock so the lookup/register decision is atomic and no
/// splice is dropped or double-applied. Matching is on logical source name only — multiple
/// physical sources expanding from the same logical name are served by a single entry per name.
class RunningSourceRegistry
{
public:
    static RunningSourceRegistry& instance();

    /// Register a live source under its logical name, then graft on any splices that arrived
    /// before it existed (drained from the pending list). Throws if a DIFFERENT RunningSource is
    /// already registered for that name; registering the SAME source is idempotent.
    /// PRECONDITION: the caller must have initialized the source's deferred-start splice budget
    /// (pendingSplices) before calling this, so drained splices count against the budget and the
    /// final one fires startEmitting().
    void registerSource(const std::string& logicalSourceName, std::weak_ptr<RunningSource> source);

    /// Remove an entry. Called from RunningSource's destructor. Looking up by logicalSourceName
    /// rather than by RunningSource* avoids stale entries from earlier queries on the same name.
    void deregisterSource(const std::string& logicalSourceName);

    /// Strict lookup: returns the live shared_ptr or nullptr if not found / expired.
    [[nodiscard]] std::shared_ptr<RunningSource> tryLookup(const std::string& logicalSourceName) const;

    /// Fire the deferred start of the source registered under `logicalSourceName`. Idempotent
    /// (no-op for sources that have already started). Returns true if a matching source was
    /// found, false otherwise.
    bool startDeferred(const std::string& logicalSourceName);

    /// Graft `successors` onto the running source for `logicalSourceName`. If that source is not
    /// registered yet (deploy-order race), the successors are stashed and grafted on later by
    /// registerSource. Atomic w.r.t. registerSource (single lock), so no splice is lost.
    void spliceOrDefer(const std::string& logicalSourceName, std::vector<std::shared_ptr<RunningQueryPlanNode>> successors);

    RunningSourceRegistry(const RunningSourceRegistry&) = delete;
    RunningSourceRegistry& operator=(const RunningSourceRegistry&) = delete;
    RunningSourceRegistry(RunningSourceRegistry&&) = delete;
    RunningSourceRegistry& operator=(RunningSourceRegistry&&) = delete;

private:
    RunningSourceRegistry() = default;
    ~RunningSourceRegistry() = default;

    struct State
    {
        std::unordered_map<std::string, std::weak_ptr<RunningSource>> entries;
        /// Splices that arrived before their source registered, keyed by logical source name.
        std::unordered_map<std::string, std::vector<std::shared_ptr<RunningQueryPlanNode>>> pendingSplices;
    };
    folly::Synchronized<State> state;
};

}
