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

#include <QueryManager/QueryManager.hpp>

#include <chrono>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <WorkerStatus.hpp>

namespace NES
{
class GRPCQuerySubmissionBackend final : public QuerySubmissionBackend
{
    std::unique_ptr<WorkerRPCService::Stub> stub;
    WorkerConfig workerConfig;

public:
    explicit GRPCQuerySubmissionBackend(WorkerConfig config);
    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(LogicalPlan) override;
    std::expected<void, Exception> start(QueryId) override;
    std::expected<void, Exception> stop(QueryId) override;
    [[nodiscard]] std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId) const override;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const override;

    /// Flips a named runtime switch on the worker. See SwitchRegistry. Used by the workload-domain
    /// adaptive swap callback to flip a gate atomic instead of stopping and redeploying the query.
    std::expected<void, Exception> setSwitch(const std::string& name, int64_t value);

    /// Registers a query plan in the worker's pending slot (compiles but does NOT deploy to the
    /// node engine). Follow up with `attachAlternatePipeline` before `start` to wrap the data
    /// pipeline stages with switchable variants. Returns the QueryId of the pending plan.
    [[nodiscard]] std::expected<QueryId, Exception> registerQueryDeferred(LogicalPlan plan) override;

    /// Compiles `alternatePlan` on the worker and merges its intermediate stages into the
    /// pending plan identified by `queryId`. Each matched stage becomes a
    /// SwitchableCompiledExecutablePipelineStage selecting between the data and alternate
    /// compiled functions via the named switch. After this returns, the data plan is committed
    /// to the node engine and can be started via `start`.
    std::expected<void, Exception> attachAlternatePipeline(
        QueryId queryId, LogicalPlan alternatePlan, const std::string& switchName, int64_t alternateExpectedValue) override;
};

BackendProvider createGRPCBackend();

}
