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

#include <chrono>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Pointers.hpp>
#include <folly/Synchronized.h>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

/// @brief The SingleNodeWorker is a compiling StreamProcessingEngine, working alone on local sources and sinks, without external
/// coordination. The SingleNodeWorker can register LogicalQueryPlans which are lowered into an executable format, by the
/// QueryCompiler. The user can manage the lifecycle of queries inside the NodeEngine using the SingleNodeWorkers interface.
/// The Class itself is NonCopyable, but Movable, it owns the QueryCompiler and the NodeEngine.
class SingleNodeWorker
{
    struct PendingPlan;
    SharedPtr<CompositeStatisticListener> listener;
    SharedPtr<NodeEngine> nodeEngine;
    UniquePtr<QueryCompilation::QueryCompiler> compiler;
    SingleNodeWorkerConfiguration configuration;
    /// Plans that have been compiled but are NOT yet handed off to NodeEngine. The workload-switch
    /// flow registers a data plan deferred, attaches an alternate later (which compiles + merges
    /// stages into SwitchableCompiledExecutablePipelineStage), and only then commits to NodeEngine.
    std::unique_ptr<folly::Synchronized<std::unordered_map<QueryId, std::shared_ptr<PendingPlan>>>> pendingPlans;

public:
    explicit SingleNodeWorker(const SingleNodeWorkerConfiguration&, const Host& = Host("SingleNodeWorker"));
    ~SingleNodeWorker();
    /// Non-Copyable
    SingleNodeWorker(const SingleNodeWorker& other) = delete;
    SingleNodeWorker& operator=(const SingleNodeWorker& other) = delete;

    /// Movable
    SingleNodeWorker(SingleNodeWorker&& other) noexcept;
    SingleNodeWorker& operator=(SingleNodeWorker&& other) noexcept;

    /// Registers a DecomposedQueryPlan which internally triggers the QueryCompiler and registers the executable query plan. Once
    /// returned the query can be started with the QueryId. The registered Query will be in the StoppedState
    /// @param plan Fully Specified LogicalQueryPlan.
    /// @return QueryId which identifies the registered Query
    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(LogicalPlan plan) noexcept;

    /// Compiles `plan` like `registerQuery` does, but holds the compiled CompiledQueryPlan in a
    /// pending slot inside this worker instead of handing it to NodeEngine. The caller is expected
    /// to follow up with `attachAlternatePipeline` to merge in a parallel filter chain (compiled
    /// independently) and commit the now-switchable plan to NodeEngine. Used by the workload-switch
    /// flow so the data query stages can be wrapped with SwitchableCompiledExecutablePipelineStage
    /// before the source threads start.
    /// @return QueryId identifying the pending plan; not yet startable.
    [[nodiscard]] std::expected<QueryId, Exception> registerQueryDeferred(LogicalPlan plan) noexcept;

    /// Compiles `alternatePlan`, pairs its intermediate pipelines with the pending plan registered
    /// under `queryId`, replaces each paired stage with a SwitchableCompiledExecutablePipelineStage
    /// that selects via the named switch in SwitchRegistry. Then commits the modified pending plan
    /// to NodeEngine. After this returns, the query is startable via `startQuery`.
    /// @param queryId identifies a plan registered via `registerQueryDeferred`.
    /// @param alternatePlan a LogicalPlan with the SAME number and shape of intermediate pipelines.
    /// @param switchName name of the switch in SwitchRegistry. Pipelines pick the alternate stage
    ///                   when the named switch equals `alternateExpectedValue`.
    /// @param alternateExpectedValue value at which the switch routes buffers to the alternate.
    std::expected<void, Exception> attachAlternatePipeline(
        QueryId queryId,
        LogicalPlan alternatePlan,
        std::string switchName,
        int64_t alternateExpectedValue = 1) noexcept;

    /// Starts the Query asynchronously and moves it into the RunningState. Query execution error are only reported during runtime
    /// of the query.
    /// @param queryId identifies the registered query
    std::expected<void, Exception> startQuery(QueryId queryId) noexcept;

    /// Stops the Query and moves it into the StoppedState. The exact semantics and guarantees depend on the chosen
    ///  QueryTerminationType
    /// @param queryId identifies the registered query
    /// @param terminationType dictates what happens with in in-flight data
    std::expected<void, Exception> stopQuery(QueryId queryId, QueryTerminationType terminationType) noexcept;

    /// Summary structure for query.
    [[nodiscard]] std::expected<LocalQueryStatusSnapshot, Exception> getQueryStatus(QueryId queryId) const noexcept;
    [[nodiscard]] WorkerStatus getWorkerStatus(std::chrono::system_clock::time_point after) const;
};
}
