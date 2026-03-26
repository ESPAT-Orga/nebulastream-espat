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
#include <cstdint>
#include <expected>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <folly/Synchronized.h>
#include <ConditionTrigger.hpp>
#include <ErrorHandling.hpp>
#include <RequestStatisticStatement.hpp>
#include <Statistic.hpp>
#include <StatisticQueryGenerator.hpp>
#include <StatisticRegistry.hpp>

namespace grpc
{
class Server;
}

namespace NES
{

/// Result of a collectNewStatistic() call.
struct CollectStatisticResult
{
    QueryId queryId;
    Statistic::StatisticId statisticId;
    bool alreadyExisted;
};

/// Central coordinator for statistic requests. Owns the StatisticRegistry and generates
/// unique StatisticIds via an atomic counter. This component can be reused by different
/// requesters (e.g., the REPL frontend, the query optimizer).
///
/// Also runs a gRPC server (StatisticCoordinatorService) that receives results from gRPC sinks.
class StatisticCoordinator
{
public:
    /// Callback that takes a LogicalPlan (already generated) and submits it.
    /// Returns the QueryId on success or an Exception on failure.
    using SubmitQueryFn = std::function<std::expected<QueryId, Exception>(LogicalPlan)>;

    StatisticCoordinator(std::unique_ptr<StatisticQueryGenerator> queryGenerator, SubmitQueryFn submitQuery);
    StatisticCoordinator(StatisticCoordinator&& other) noexcept;
    StatisticCoordinator& operator=(StatisticCoordinator&& other) noexcept;
    ~StatisticCoordinator();

    /// Requests collection of a new statistic. If an identical request is already active (same metric,
    /// collection domain, window size), returns the existing entry (and appends the trigger if provided).
    /// Otherwise generates a unique StatisticId, creates the collection query, submits it, and registers the entry.
    [[nodiscard]] std::expected<CollectStatisticResult, Exception> collectNewStatistic(const RequestStatisticBuildStatement& statement);

    /// Adds a condition trigger to an existing statistic entry.
    /// Returns false if the key is not found in the registry.
    bool addConditionTrigger(const StatisticRegistry::Key& key, ConditionTrigger trigger);

    /// Removes the entry for this key. Returns true if an entry was removed.
    bool deregisterStatistic(const StatisticRegistry::Key& key);

    /// Starts the gRPC server on a dynamic port. Returns the "host:port" string.
    std::string startGrpcServer();

    /// Stops the gRPC server.
    void stopGrpcServer();

    /// Returns the coordinator's gRPC address ("host:port").
    [[nodiscard]] const std::string& getCoordinatorAddress() const { return coordinatorAddress; }

    /// Submits a probe query and waits for the result.
    /// @param keys The statistic registry keys to probe.
    /// @param startTs Start of the time range.
    /// @param endTs End of the time range.
    /// @param probeQueryWithoutSource A logical plan containing probe operators but no source or sink.
    /// @return The aggregated probe result, or std::nullopt if no result was received in time.
    std::optional<double> getStatistics(
        const std::vector<StatisticRegistry::Key>& keys,
        Windowing::TimeMeasure startTs,
        Windowing::TimeMeasure endTs,
        LogicalPlan& probeQueryWithoutSource);

    /// Called by the gRPC service handler when a StatisticReport arrives.
    /// Routes to pending probes or condition triggers.
    void onStatisticReport(Statistic::StatisticId statisticId, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs, double value);

private:
    std::atomic<uint64_t> nextStatisticId{1};
    StatisticRegistry registry;
    std::unique_ptr<StatisticQueryGenerator> queryGenerator;
    SubmitQueryFn submitQuery;

    /// gRPC server for receiving results from sinks.
    std::unique_ptr<grpc::Server> grpcServer;
    std::string coordinatorAddress;

    /// Pending probe queries waiting for results. Keyed by raw statisticId value.
    struct PendingProbe
    {
        std::promise<double> promise;
    };

    folly::Synchronized<std::unordered_map<Statistic::StatisticId, PendingProbe>> pendingProbes;
};

}
