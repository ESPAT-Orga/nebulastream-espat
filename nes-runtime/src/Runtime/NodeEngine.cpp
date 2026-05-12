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

#include <Runtime/NodeEngine.hpp>

#include <chrono>
#include <memory>
#include <thread>
#include <unordered_map>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/AtomicState.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngine.hpp>
#include <QueryStatus.hpp>

namespace NES
{

class QueryTracker
{
    /// The CompiledQueryPlan is retained as shared_ptr in both states so that a Executing→Idle
    /// transition (after stopQuery) can restore it without re-compiling. This is what allows the
    /// adaptive optimizer to swap between pre-compiled query variants with O(1) cost.
    struct Idle
    {
        std::shared_ptr<CompiledQueryPlan> qep;
    };

    struct Executing
    {
        std::shared_ptr<CompiledQueryPlan> qep;
    };

    using QueryState = AtomicState<Idle, Executing>;
    folly::Synchronized<std::unordered_map<QueryId, std::unique_ptr<QueryState>>> queries;

public:
    void registerQuery(std::unique_ptr<CompiledQueryPlan> qep, QueryId queryId)
    {
        auto locked = queries.wlock();
        auto [it, inserted]
            = locked->emplace(queryId, std::make_unique<QueryState>(Idle{std::shared_ptr<CompiledQueryPlan>(std::move(qep))}));
        if (!inserted)
        {
            throw QueryAlreadyRegistered("Query with ID {} is already registered", queryId);
        }
    }

    /// Move from Idle to Executing. Returns a shared_ptr to the CompiledQueryPlan (also retained in
    /// the new Executing state). Returns null if the query isn't currently Idle (e.g. already
    /// executing, not registered).
    std::shared_ptr<CompiledQueryPlan> moveToExecuting(QueryId qid)
    {
        auto rlocked = queries.rlock();
        std::shared_ptr<CompiledQueryPlan> qep;
        if (auto it = rlocked->find(qid); it != rlocked->end())
        {
            it->second->transition(
                [&](Idle&& idle)
                {
                    qep = idle.qep;
                    return Executing{std::move(idle.qep)};
                });
        }
        return qep;
    }

    /// Move from Executing back to Idle. Used by stopQuery so the same queryId can be started
    /// again later, reusing the cached compiled pipeline functions inside the CompiledQueryPlan.
    /// No-op if the query isn't currently Executing.
    void moveBackToIdle(QueryId qid)
    {
        auto rlocked = queries.rlock();
        if (auto it = rlocked->find(qid); it != rlocked->end())
        {
            it->second->transition([](Executing&& executing) { return Idle{std::move(executing.qep)}; });
        }
    }
};

NodeEngine::~NodeEngine()
{
    NES_DEBUG("Shutting down NodeEngine");
    queryEngine.reset();
    sourceProvider.reset();
    queryTracker.reset();

    bufferManager->destroy();
    bufferManager.reset();
}

NodeEngine::NodeEngine(
    std::shared_ptr<BufferManager> bufferManager,
    std::shared_ptr<SystemEventListener> systemEventListener,
    std::shared_ptr<QueryLog> queryLog,
    std::unique_ptr<QueryEngine> queryEngine,
    std::unique_ptr<SourceProvider> sourceProvider,
    std::shared_ptr<AbstractStatisticStore> statisticStore)
    : bufferManager(std::move(bufferManager))
    , queryLog(std::move(queryLog))
    , systemEventListener(std::move(systemEventListener))
    , queryEngine(std::move(queryEngine))
    , queryTracker(std::make_unique<QueryTracker>())
    , sourceProvider(std::move(sourceProvider))
    , statisticStore(std::move(statisticStore))
{
}

void NodeEngine::registerCompiledQueryPlan(QueryId queryId, std::unique_ptr<CompiledQueryPlan> compiledQueryPlan)
{
    queryTracker->registerQuery(std::move(compiledQueryPlan), queryId);
    queryLog->logQueryStatusChange(queryId, QueryStatus::Registered, std::chrono::system_clock::now());
}

void NodeEngine::startQuery(QueryId queryId)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");

    if (auto qep = queryTracker->moveToExecuting(queryId))
    {
        systemEventListener->onEvent(StartQuerySystemEvent(queryId));
        queryEngine->start(ExecutableQueryPlan::instantiate(*qep, *sourceProvider));
    }
    else
    {
        throw QueryNotRegistered("Query with queryId {} is not currently idle", queryId);
    }
}

void NodeEngine::stopQuery(QueryId queryId, QueryTerminationType)
{
    PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
    NES_INFO("Stop {}", queryId);
    systemEventListener->onEvent(StopQuerySystemEvent(queryId));

    /// Capture the time before triggering stop so we can recognise the resulting status event
    /// (the queryLog tracks the *latest* Stopped/Failed timestamp; this lets us distinguish a
    /// new stop from one accumulated by a prior execution of the same queryId).
    const auto stopRequestTime = std::chrono::system_clock::now();
    queryEngine->stop(queryId);

    /// Wait for the engine to actually finish stopping before transitioning the tracker back to
    /// Idle. Without this, a subsequent startQuery on the same queryId could race with the still-
    /// running source teardown. We poll the queryLog because the engine's stop is asynchronous;
    /// once a Stopped/Failed status is recorded the engine has released the executing resources.
    constexpr auto pollInterval = std::chrono::milliseconds(5);
    constexpr size_t maxPolls = 2000; /// up to ~10s total
    for (size_t i = 0; i < maxPolls; ++i)
    {
        if (const auto snapshot = queryLog->getQueryStatus(queryId);
            snapshot && (snapshot->state == QueryStatus::Stopped || snapshot->state == QueryStatus::Failed)
            && snapshot->metrics.stop && *snapshot->metrics.stop >= stopRequestTime)
        {
            queryTracker->moveBackToIdle(queryId);
            return;
        }
        std::this_thread::sleep_for(pollInterval);
    }
    NES_WARNING("stopQuery: timed out waiting for {} to reach Stopped state; tracker remains Executing", queryId);
}

}
