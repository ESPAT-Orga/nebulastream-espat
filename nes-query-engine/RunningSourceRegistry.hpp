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

/// Worker-wide registry mapping logical source name -> live RunningSource. Populated by the
/// RunningQueryPlan setup callback after a non-splice source is created, cleared via
/// RunningSource's destructor. Used by the SpliceToRunningSourceTrait code path: when a query
/// arrives with that trait on its source, it looks up the running source for the same logical
/// name and appends its head-pipeline nodes there instead of spawning a fresh source thread.
/// Strict: lookup fails if there is no matching live source, and matches MUST be unique. The
/// matching semantics on logical source name only — multiple physical sources expanding from
/// the same logical name are still served by a single registry entry per name.
class RunningSourceRegistry
{
public:
    static RunningSourceRegistry& instance();

    /// Register a live source under its logical name. Throws if a different RunningSource is
    /// already registered for that name — concurrent registrations of the SAME RunningSource
    /// are idempotent. Returns immediately; the splice path only sees registrations that have
    /// completed.
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

    RunningSourceRegistry(const RunningSourceRegistry&) = delete;
    RunningSourceRegistry& operator=(const RunningSourceRegistry&) = delete;
    RunningSourceRegistry(RunningSourceRegistry&&) = delete;
    RunningSourceRegistry& operator=(RunningSourceRegistry&&) = delete;

private:
    RunningSourceRegistry() = default;
    ~RunningSourceRegistry() = default;

    folly::Synchronized<std::unordered_map<std::string, std::weak_ptr<RunningSource>>> entries;
};

}
