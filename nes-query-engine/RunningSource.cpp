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
#include <iostream>
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
#include <fmt/format.h>
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
    size_t numberOfInflightBuffers,
    std::weak_ptr<RunningSource> source,
    std::shared_ptr<RunningSource::SuccessorContainer> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    auto availableBuffer = std::make_shared<std::counting_semaphore<>>(
        std::min(numberOfInflightBuffers, static_cast<size_t>(std::numeric_limits<int32_t>::max())));
    return [&controller, successors = std::move(successors), source, &emitter, queryId, availableBuffer = std::move(availableBuffer)](
               const OriginId sourceId,
               SourceReturnType::SourceReturnType event,
               const std::stop_token& stopToken) -> SourceReturnType::EmitResult
    {
        return std::visit(
            Overloaded{
                [&](const SourceReturnType::Data& data)
                {
                    /// Snapshot the successor list under the registry's read-lock so the loop body
                    /// can iterate without holding the lock. Splice-time appends are seen on the
                    /// next buffer.
                    const auto snapshot = successors->copy();
                    for (const auto& successor : snapshot)
                    {
                        {
                            /// release the semaphore in case the source wants to terminate
                            const std::stop_callback callback(stopToken, [&]() { availableBuffer->release(); });
                            availableBuffer->acquire();
                            if (stopToken.stop_requested())
                            {
                                return SourceReturnType::EmitResult::STOP_REQUESTED;
                            }
                        }
                        /// The admission queue might be full, we have to reattempt
                        while (not emitter.emitWork(
                            queryId,
                            successor,
                            data.buffer,
                            TaskCallback{TaskCallback::OnComplete([availableBuffer] { availableBuffer->release(); })},
                            PipelineExecutionContext::ContinuationPolicy::NEVER))
                        {
                            if (stopToken.stop_requested())
                            {
                                return SourceReturnType::EmitResult::STOP_REQUESTED;
                            }
                        }
                        ENGINE_LOG_DEBUG("Source Emitted Data to successor: {}-{}", queryId, successor->id);
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
    std::vector<std::shared_ptr<RunningQueryPlanNode>> initialSuccessors,
    std::unique_ptr<SourceHandle> source,
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
    std::function<void(Exception)> onSourceFailure,
    std::string logicalSourceName)
    : successors(std::make_shared<SuccessorContainer>(std::move(initialSuccessors)))
    , source(std::move(source))
    , onSourceStopped(std::move(onSourceStopped))
    , onSourceFailure(std::move(onSourceFailure))
    , logicalSourceName(std::move(logicalSourceName))
{
}

std::shared_ptr<RunningSource> RunningSource::create(
    QueryId queryId,
    std::unique_ptr<SourceHandle> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
    std::function<void(Exception)> onSourceFailure,
    QueryLifetimeController& controller,
    WorkEmitter& emitter,
    std::string logicalSourceName)
{
    const auto maxInflightBuffers = source->getRuntimeConfiguration().inflightBufferLimit;
    auto runningSource = std::shared_ptr<RunningSource>(new RunningSource(
        std::move(successors), std::move(source), std::move(onSourceStopped), std::move(onSourceFailure), logicalSourceName));
    ENGINE_LOG_DEBUG("Starting Running Source");
    if (not logicalSourceName.empty())
    {
        /// Register before starting the source thread so the splice path can find us as soon as
        /// the source begins emitting. Deregistration is in ~RunningSource.
        RunningSourceRegistry::instance().registerSource(logicalSourceName, std::weak_ptr<RunningSource>(runningSource));
    }
    {
        const std::scoped_lock lock(runningSource->mutex);
        runningSource->source->start(emitFunction(queryId, maxInflightBuffers, runningSource, runningSource->successors, controller, emitter));
    }
    return runningSource;
}

void RunningSource::appendSuccessors(std::vector<std::shared_ptr<RunningQueryPlanNode>> additionalSuccessors)
{
    if (additionalSuccessors.empty())
    {
        return;
    }
    auto locked = successors->wlock();
    std::cout << fmt::format(
        "[SOURCE_SPLICE] appending {} successor pipelines to running source for logical source '{}'\n",
        additionalSuccessors.size(),
        logicalSourceName);
    std::cout.flush();
    locked->insert(locked->end(), std::make_move_iterator(additionalSuccessors.begin()), std::make_move_iterator(additionalSuccessors.end()));
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

    std::vector<std::shared_ptr<RunningQueryPlanNode>> drained;
    {
        auto locked = this->successors->wlock();
        drained = std::move(*locked);
        locked->clear();
    }
    if (onSourceStopped(std::move(drained)))
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
