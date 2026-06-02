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

#include <RunningSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <semaphore>
#include <stop_token>
#include <string>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Overloaded.hpp>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <Interfaces.hpp>
#include <PipelineExecutionContext.hpp>
#include <RunningQueryPlan.hpp>
#include <RunningSourceRegistry.hpp>

namespace NES
{

namespace
{
SourceReturnType::EmitFunction emitFunction(
    QueryId queryId,
    std::weak_ptr<RunningSource> source,
    std::shared_ptr<RunningSource::SuccessorContainer> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    return [&controller, successors = std::move(successors), source, &emitter, queryId](
               const OriginId sourceId,
               SourceReturnType::SourceReturnType event,
               const std::stop_token& stopToken) -> SourceReturnType::EmitResult
    {
        return std::visit(
            Overloaded{
                [&](const SourceReturnType::Data& data)
                {
                    /// Snapshot the successor list under the read-lock so the loop body can iterate
                    /// without holding the lock. Splice-time appends are seen on the next buffer.
                    /// Each entry carries its own per-successor semaphore — a slow build-branch
                    /// pipeline will only backpressure ITS OWN slot pool, not the data path's.
                    const auto snapshot = successors->copy();
                    for (const auto& entry : snapshot)
                    {
                        auto slots = entry.availableSlots;
                        {
                            /// release the semaphore in case the source wants to terminate
                            const std::stop_callback callback(stopToken, [&]() { slots->release(); });
                            slots->acquire();
                            if (stopToken.stop_requested())
                            {
                                return SourceReturnType::EmitResult::STOP_REQUESTED;
                            }
                        }
                        /// The admission queue might be full, we have to reattempt
                        while (not emitter.emitWork(
                            queryId,
                            entry.node,
                            data.buffer,
                            TaskCallback{TaskCallback::OnComplete([slots] { slots->release(); })},
                            PipelineExecutionContext::ContinuationPolicy::NEVER))
                        {
                            if (stopToken.stop_requested())
                            {
                                return SourceReturnType::EmitResult::STOP_REQUESTED;
                            }
                        }
                        ENGINE_LOG_DEBUG("Source Emitted Data to successor: {}-{}", queryId, entry.node->id);
                    }
                    return SourceReturnType::EmitResult::SUCCESS;
                },
                [&](SourceReturnType::EoS)
                {
                    ENGINE_LOG_DEBUG("Source with OriginId {} reached end of stream for query {}", sourceId, queryId);
                    controller.initializeSourceStop(queryId, sourceId, source);
                    return SourceReturnType::EmitResult::SUCCESS;
                },
                [&](SourceReturnType::Error error)
                {
                    controller.initializeSourceFailure(queryId, sourceId, source, std::move(error.ex));
                    return SourceReturnType::EmitResult::SUCCESS;
                }},
            std::move(event));
    };
}
}

OriginId RunningSource::getOriginId() const
{
    return source->getSourceId();
}

RunningSource::RunningSource(
    std::vector<SuccessorEntry> initialSuccessors,
    std::unique_ptr<SourceHandle> source,
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
    std::function<void(Exception)> onSourceFailure,
    std::string logicalSourceName,
    size_t inflightBufferLimit)
    : successors(std::make_shared<SuccessorContainer>(std::move(initialSuccessors)))
    , source(std::move(source))
    , onSourceStopped(std::move(onSourceStopped))
    , onSourceFailure(std::move(onSourceFailure))
    , logicalSourceName(std::move(logicalSourceName))
    , inflightBufferLimit(inflightBufferLimit)
{
}

namespace
{
RunningSource::SuccessorEntry makeEntry(std::shared_ptr<RunningQueryPlanNode> node, size_t inflightBufferLimit)
{
    const auto slotCount = std::min(inflightBufferLimit, static_cast<size_t>(std::numeric_limits<int32_t>::max()));
    return RunningSource::SuccessorEntry{
        .node = std::move(node), .availableSlots = std::make_shared<std::counting_semaphore<>>(slotCount)};
}
}

std::shared_ptr<RunningSource> RunningSource::create(
    QueryId queryId,
    std::unique_ptr<SourceHandle> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
    std::function<void(Exception)> onSourceFailure,
    QueryLifetimeController& controller,
    WorkEmitter& emitter,
    std::string logicalSourceName,
    bool deferStart,
    uint32_t expectedSpliceCount)
{
    const auto maxInflightBuffers = source->getRuntimeConfiguration().inflightBufferLimit;
    std::vector<SuccessorEntry> initialEntries;
    initialEntries.reserve(successors.size());
    for (auto& node : successors)
    {
        initialEntries.push_back(makeEntry(std::move(node), maxInflightBuffers));
    }
    auto runningSource = std::shared_ptr<RunningSource>(new RunningSource(
        std::move(initialEntries),
        std::move(source),
        std::move(onSourceStopped),
        std::move(onSourceFailure),
        logicalSourceName,
        maxInflightBuffers));
    ENGINE_LOG_DEBUG("Starting Running Source");
    if (not logicalSourceName.empty())
    {
        /// Register before starting the source thread so the splice path can find us as soon as
        /// the source begins emitting. Deregistration is in ~RunningSource.
        RunningSourceRegistry::instance().registerSource(logicalSourceName, std::weak_ptr<RunningSource>(runningSource));
    }
    /// Build the start closure once. In the immediate-start path we invoke it now; in the
    /// deferred-start path we stash it on the RunningSource and an external trigger fires it.
    auto startFn
        = [&controller, &emitter, queryId, weakSource = std::weak_ptr<RunningSource>(runningSource)]()
    {
        if (auto self = weakSource.lock())
        {
            const std::scoped_lock lock(self->mutex);
            self->source->start(emitFunction(queryId, self, self->successors, controller, emitter));
        }
    };
    if (deferStart)
    {
        runningSource->pendingSplices.store(std::max<uint32_t>(expectedSpliceCount, 1));
        runningSource->deferredStart = std::move(startFn);
    }
    else
    {
        runningSource->started.store(true);
        startFn();
    }
    return runningSource;
}

void RunningSource::startEmitting()
{
    if (started.exchange(true))
    {
        /// already started — idempotent
        return;
    }
    std::function<void()> toFire;
    {
        const std::scoped_lock lock(mutex);
        toFire = std::move(deferredStart);
        deferredStart = {};
    }
    if (toFire)
    {
        toFire();
    }
}

void RunningSource::appendSuccessors(std::vector<std::shared_ptr<RunningQueryPlanNode>> additionalSuccessors)
{
    if (additionalSuccessors.empty())
    {
        return;
    }
    {
        auto locked = successors->wlock();
        for (auto& node : additionalSuccessors)
        {
            locked->push_back(makeEntry(std::move(node), inflightBufferLimit));
        }
    }
    /// Count this splice against the pending budget set by the deferStart path. If the budget
    /// hits 0, fire the deferred start. Non-deferred sources have pendingSplices == 0 from
    /// creation, so this is a no-op.
    uint32_t previous = pendingSplices.load();
    while (previous > 0 && !pendingSplices.compare_exchange_weak(previous, previous - 1))
    {
        /// retry
    }
    if (previous > 0 && pendingSplices.load() == 0)
    {
        startEmitting();
    }
}

RunningSource::~RunningSource()
{
    if (not logicalSourceName.empty())
    {
        RunningSourceRegistry::instance().deregisterSource(logicalSourceName);
    }
    if (source)
    {
        ENGINE_LOG_DEBUG("Stopping Running Source");
        if (source->tryStop(std::chrono::milliseconds(0)) == SourceReturnType::TryStopResult::TIMEOUT)
        {
            ENGINE_LOG_DEBUG("Source was requested to stop. Stop will happen asynchronously.");
        }
    }
}

bool RunningSource::attemptUnregister()
{
    const auto result = tryStop();
    if (result == SourceReturnType::TryStopResult::NOT_RUNNING)
    {
        /// Source was already stopped, callback was already called
        return true;
    }
    if (result != SourceReturnType::TryStopResult::SUCCESS)
    {
        return false;
    }

    std::vector<std::shared_ptr<RunningQueryPlanNode>> drainedNodes;
    {
        auto locked = this->successors->wlock();
        drainedNodes.reserve(locked->size());
        for (auto& entry : *locked)
        {
            drainedNodes.push_back(std::move(entry.node));
        }
        locked->clear();
    }
    if (onSourceStopped(std::move(drainedNodes)))
    {
        return true;
    }
    return false;
}

SourceReturnType::TryStopResult RunningSource::tryStop() const
{
    const std::scoped_lock lock(mutex);
    return this->source->tryStop(std::chrono::milliseconds(0));
}

void RunningSource::fail(Exception exception) const
{
    onSourceFailure(std::move(exception));
}

}
