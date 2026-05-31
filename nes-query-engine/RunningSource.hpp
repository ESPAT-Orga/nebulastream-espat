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
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceHandle.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>
#include <Interfaces.hpp>

namespace NES
{
struct RunningQueryPlan;
struct RunningQueryPlanNode;

/// The Running Source is a wrapper around the SourceHandle. The Lifecycle of the RunningSource controls start/stop of the source handle.
/// The purpose of the running source is to create the emit function which and redirects external events towards the task queue, where one of
/// the WorkerThreads can handle them. We cannot allow that the SourceThread causes the RunningSource to be destroyed, which would cause a
/// deadlock. Additionally, the RunningSource prevents the SourceThread from accidentally triggering the successor pipeline termination, by
/// saving references to the successors. Only if both the SourceThread and the RunningSource are destroyed are successor pipelines terminated.
/// Starting and Stopping of the SourceThread is done asynchronously. Destroying the RunningSource guarantees that the SourceThread was
/// requested to stop, however it might still be active.
class RunningSource
{
public:
    /// Creates and starts the underlying source implementation. As long as the RunningSource is kept alive the source will run,
    /// once the last reference to the RunningSource is destroyed the source is stopped.
    /// The onSourceStopped callback is invoked after the source has been successfully stopped.
    /// If logicalSourceName is non-empty, the constructed RunningSource registers itself in the
    /// process-wide RunningSourceRegistry under that name and deregisters on destruction. This is
    /// what later splice-mode queries look up to graft their successors onto this source.
    static std::shared_ptr<RunningSource> create(
        QueryId queryId,
        std::unique_ptr<SourceHandle> source,
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
        std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
        std::function<void(Exception)> onSourceFailure,
        QueryLifetimeController& controller,
        WorkEmitter& emitter,
        std::string logicalSourceName = {});

    /// Append additional head pipelines to this source's emit fan-out. Used by the splice path so
    /// a later query's build branch can run off the same source thread as the data query.
    void appendSuccessors(std::vector<std::shared_ptr<RunningQueryPlanNode>> additionalSuccessors);

    RunningSource(const RunningSource& other) = delete;
    RunningSource& operator=(const RunningSource& other) = delete;
    RunningSource(RunningSource&& other) noexcept = delete;
    RunningSource& operator=(RunningSource&& other) noexcept = delete;

    ~RunningSource();
    [[nodiscard]] OriginId getOriginId() const;

    bool attemptUnregister();
    void fail(Exception exception) const;

    /// Calls the underlying `tryStop`
    [[nodiscard]] SourceReturnType::TryStopResult tryStop() const;

    /// Shared-mutable container type used by the emit closure. Held by shared_ptr so the closure
    /// keeps it alive as long as the source is emitting; mutated under folly::Synchronized so the
    /// splice path can append after creation without racing with the emit fast path.
    using SuccessorContainer = folly::Synchronized<std::vector<std::shared_ptr<RunningQueryPlanNode>>>;

private:
    RunningSource(
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
        std::unique_ptr<SourceHandle> source,
        std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
        std::function<void(Exception)> onSourceFailure,
        std::string logicalSourceName);

    mutable std::mutex mutex; /// Protects against race between create() (starting the source) and tryStop() (stopping the source)
    /// shared_ptr so the emit closure (created in create() below) reads from the SAME container
    /// that appendSuccessors() later mutates.
    std::shared_ptr<SuccessorContainer> successors;
    std::unique_ptr<SourceHandle> source;
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped;
    std::function<void(Exception)> onSourceFailure;
    std::string logicalSourceName;
};

}
