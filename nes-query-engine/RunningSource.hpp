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
#include <functional>
#include <memory>
#include <mutex>
#include <semaphore>
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
    /// If deferStart is true, the source is constructed and registered but its underlying emit
    /// thread is NOT started; a subsequent startEmitting() call (typically via
    /// RunningSourceRegistry::startDeferred(name)) starts it. Used so splice pipelines can wire
    /// in before sequence 0 is emitted.
    static std::shared_ptr<RunningSource> create(
        QueryId queryId,
        std::unique_ptr<SourceHandle> source,
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
        std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
        std::function<void(Exception)> onSourceFailure,
        QueryLifetimeController& controller,
        WorkEmitter& emitter,
        std::string logicalSourceName = {},
        bool deferStart = false,
        uint32_t expectedSpliceCount = 1);

    /// Append additional head pipelines to this source's emit fan-out. Used by the splice path so
    /// a later query's build branch can run off the same source thread as the data query.
    void appendSuccessors(std::vector<std::shared_ptr<RunningQueryPlanNode>> additionalSuccessors);

    /// Starts the underlying source's emit thread. No-op if already started (idempotent).
    /// Intended for the deferStart=true case: call after all expected splices have completed.
    void startEmitting();

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

    /// One entry per successor pipeline: the head RunningQueryPlanNode and a *per-successor*
    /// inflight-tasks semaphore. Earlier versions of this code used a single shared semaphore for
    /// all successors, which caused the data query to slow down 3× when the workload-domain build
    /// branch was spliced in — every buffer consumed N slots from a 64-slot pool, and the slower
    /// branch tied up slots that the data path needed. With per-successor semaphores each branch
    /// is independently rate-limited and the data path no longer waits on the build branch.
    struct SuccessorEntry
    {
        std::shared_ptr<RunningQueryPlanNode> node;
        std::shared_ptr<std::counting_semaphore<>> availableSlots;
    };
    using SuccessorContainer = folly::Synchronized<std::vector<SuccessorEntry>>;

private:
    RunningSource(
        std::vector<SuccessorEntry> successors,
        std::unique_ptr<SourceHandle> source,
        std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
        std::function<void(Exception)> onSourceFailure,
        std::string logicalSourceName,
        size_t inflightBufferLimit);

    /// One-shot startup closure populated when deferStart=true. startEmitting() moves it out
    /// under the mutex; second/later calls find it empty and become no-ops. atomic_bool guards
    /// fast-path "already-started" reads without acquiring the mutex.
    std::atomic_bool started{false};
    std::function<void()> deferredStart;
    /// Number of splices remaining before the deferred start fires. Counts down inside
    /// appendSuccessors(); reaching 0 triggers startEmitting(). 0 from creation means "no
    /// deferral, start immediately" (handled in create()).
    std::atomic<uint32_t> pendingSplices{0};

    mutable std::mutex mutex; /// Protects against race between create() (starting the source) and tryStop() (stopping the source)
    /// shared_ptr so the emit closure (created in create() below) reads from the SAME container
    /// that appendSuccessors() later mutates.
    std::shared_ptr<SuccessorContainer> successors;
    std::unique_ptr<SourceHandle> source;
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped;
    std::function<void(Exception)> onSourceFailure;
    std::string logicalSourceName;
    /// Slot count for each successor's per-successor semaphore; pinned at create time so later
    /// splice-time appendSuccessors() calls allocate semaphores with the same budget.
    size_t inflightBufferLimit{0};
};

}
