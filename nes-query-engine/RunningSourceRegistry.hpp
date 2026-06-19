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
/// Splice and registration race: a build branch can finish pipeline setup and try to splice
/// BEFORE the data query has registered its source (the data query compiles slower — it carries
/// the heavy filter chain). To make deployment order irrelevant, a splice that finds no live
/// source is QUEUED in `pendingSplices` and grafted automatically when the source later registers.
/// The entries map and the pending-splice queue live under a single lock so the "is the source
/// registered?" check and the enqueue are atomic with respect to registration + drain.
class RunningSourceRegistry
{
public:
    static RunningSourceRegistry& instance();

    /// Splice now, or queue until the target registers. If a live source is registered under
    /// `logicalSourceName`, append `successorNodes` to it immediately and return true. Otherwise
    /// move them into the pending-splice queue (grafted later by registerSourceAndDrain) and
    /// return false. Never throws, never fails — deployment order does not matter.
    bool spliceOrEnqueue(const std::string& logicalSourceName, std::vector<std::shared_ptr<RunningQueryPlanNode>>&& successorNodes);

    /// Register a live source under its logical name and graft any splice batches that were queued
    /// before it registered. Throws if a DIFFERENT RunningSource is already registered for that
    /// name — concurrent registrations of the SAME RunningSource are idempotent. `source` must be
    /// the strong ref from RunningSource::create with its pendingSplices budget already installed,
    /// so each drained appendSuccessors counts the budget down correctly.
    void registerSourceAndDrain(const std::string& logicalSourceName, const std::shared_ptr<RunningSource>& source);

    /// Remove an entry. Called from RunningSource's destructor. Looking up by logicalSourceName
    /// rather than by RunningSource* avoids stale entries from earlier queries on the same name.
    /// Also reaps any still-queued splice batches for that name when the legitimate owner expires.
    void deregisterSource(const std::string& logicalSourceName);

    /// Strict lookup: returns the live shared_ptr or nullptr if not found / expired.
    [[nodiscard]] std::shared_ptr<RunningSource> tryLookup(const std::string& logicalSourceName) const;

    /// Fire the deferred start of the source registered under `logicalSourceName`. Idempotent
    /// (no-op for sources that have already started). Returns true if a matching source was
    /// found, false otherwise.
    bool startDeferred(const std::string& logicalSourceName);

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
        /// Splice batches that arrived before their target source registered, keyed by logical
        /// source name. Each batch is one spliceOrEnqueue call's successor nodes. Drained (and the
        /// key erased) by registerSourceAndDrain; reaped by deregisterSource.
        std::unordered_map<std::string, std::vector<std::vector<std::shared_ptr<RunningQueryPlanNode>>>> pendingSplices;
    };

    folly::Synchronized<State> state;
};

}
