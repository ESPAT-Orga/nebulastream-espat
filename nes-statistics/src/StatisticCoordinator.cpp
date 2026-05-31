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

#include <StatisticCoordinator.hpp>

#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Traits/DeferSourceStartTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/status.h>
#include <ConditionTrigger.hpp>
#include <ErrorHandling.hpp>
#include <RequestStatisticStatement.hpp>
#include <Statistic.hpp>
#include <StatisticQueryGenerator.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>

namespace NES
{
const auto *const addressAndPort = "0.0.0.0:0";

/// gRPC service implementation that routes incoming reports to the StatisticCoordinator.
class StatisticCoordinatorServiceImpl final : public StatisticCoordinatorService::Service
{
public:
    explicit StatisticCoordinatorServiceImpl(StatisticCoordinator& coordinator) : coordinator(coordinator) { }

    grpc::Status ReportStatistic(grpc::ServerContext*, const StatisticReport* report, google::protobuf::Empty*) override
    {
        coordinator.onStatisticReport(
            Statistic::StatisticId{report->statistic_id()},
            Windowing::TimeMeasure{report->start_ts()},
            Windowing::TimeMeasure{report->end_ts()},
            report->value());
        return grpc::Status::OK;
    }

private:
    StatisticCoordinator& coordinator;
};

StatisticCoordinator::StatisticCoordinator(std::unique_ptr<StatisticQueryGenerator> queryGenerator, SubmitQueryFn submitQuery)
    : queryGenerator(std::move(queryGenerator)), submitQuery(std::move(submitQuery))
{
}

StatisticCoordinator::StatisticCoordinator(StatisticCoordinator&& other) noexcept
    : nextStatisticId(other.nextStatisticId.load())
    , registry(std::move(other.registry))
    , queryGenerator(std::move(other.queryGenerator))
    , submitQuery(std::move(other.submitQuery))
    , grpcServer(std::move(other.grpcServer))
    , coordinatorAddress(std::move(other.coordinatorAddress))
{
}

StatisticCoordinator& StatisticCoordinator::operator=(StatisticCoordinator&& other) noexcept
{
    nextStatisticId.store(other.nextStatisticId.load());
    registry = std::move(other.registry);
    queryGenerator = std::move(other.queryGenerator);
    submitQuery = std::move(other.submitQuery);
    grpcServer = std::move(other.grpcServer);
    coordinatorAddress = std::move(other.coordinatorAddress);
    return *this;
}

StatisticCoordinator::~StatisticCoordinator()
{
    stopGrpcServer();
}

std::expected<CollectStatisticResult, Exception> StatisticCoordinator::collectNewStatistic(const RequestStatisticBuildStatement& statement)
{
    const StatisticRegistry::Key key{
        .metric = statement.metric, .collectionDomain = statement.domain, .windowSize = Windowing::TimeMeasure{statement.windowSizeMs}};

    if (const auto existing = registry.find(key))
    {
        if (statement.conditionTrigger.has_value())
        {
            registry.addTrigger(key, statement.conditionTrigger.value());
        }
        return CollectStatisticResult{.queryId = existing->queryId, .statisticId = existing->statisticId, .alreadyExisted = true};
    }

    const auto statisticId = Statistic::StatisticId{nextStatisticId.fetch_add(1)};
    auto plan = queryGenerator->generateQuery(statement, statisticId, coordinatorAddress);

    return submitQuery(std::move(plan))
        .transform(
            [this, &key, statisticId, &statement](auto queryId)
            {
                std::vector<ConditionTrigger> triggers;
                if (statement.conditionTrigger.has_value())
                {
                    triggers.emplace_back(*statement.conditionTrigger);
                }
                registry.registerStatistic(key, queryId, statisticId, std::move(triggers));
                return CollectStatisticResult{.queryId = queryId, .statisticId = statisticId, .alreadyExisted = false};
            });
}

std::expected<CollectStatisticResult, Exception> StatisticCoordinator::collectWorkloadStatistic(
    const RequestStatisticBuildStatement& statement,
    const LogicalPlan& dataQueryPlan,
    const std::function<std::expected<QueryId, Exception>(LogicalPlan)>& submitPlan,
    const uint64_t probeIntervalMs)
{
    const auto* domain = std::get_if<WorkloadDomain>(&statement.domain);
    if (domain == nullptr)
    {
        return std::unexpected(InvalidConfigParameter("collectWorkloadStatistic requires a WorkloadDomain in the request"));
    }

    /// MVP: assume the data query has exactly one SourceNameLogicalOperator and splice the build
    /// branch as a sibling consumer of that operator. This matches the adaptive-optimization use
    /// case (single Memory source feeding a filter chain). Multi-source / join-shaped data queries
    /// would need a richer splice (matching against domain.operatorId explicitly).
    const auto sources = getOperatorByType<SourceNameLogicalOperator>(dataQueryPlan);
    if (sources.empty())
    {
        return std::unexpected(InvalidConfigParameter("WorkloadDomain splice: data query has no source-name operator"));
    }
    if (sources.size() != 1)
    {
        return std::unexpected(NotImplemented(
            "WorkloadDomain splice MVP requires the data query to have exactly one source (got {})", sources.size()));
    }
    const LogicalOperator spliceLeaf{sources.front()};

    const StatisticRegistry::Key key{
        .metric = statement.metric, .collectionDomain = statement.domain, .windowSize = Windowing::TimeMeasure{statement.windowSizeMs}};

    const auto hostIt = statement.options.find("host");
    const auto& sinkWorkerHost = hostIt != statement.options.end() ? hostIt->second : std::string{"localhost:8080"};

    /// Deploy one gated probe pipeline (Generator → EquiWidthHistogramProbe → Selection(predicate)
    /// → Projection(STATISTICID := regimeId) → GrpcSink) and register the trigger's callback under
    /// the regimeId so that pipeline's reports route to that callback.
    /// Returns the freshly allocated regimeId on success.
    auto deployGatedProbe =
        [&](const Statistic::StatisticId buildStatisticId, const ConditionTrigger& trigger) -> std::optional<Statistic::StatisticId>
    {
        if (not trigger.condition.has_value())
        {
            return std::nullopt;
        }
        const auto regimeId = Statistic::StatisticId{nextStatisticId.fetch_add(1)};
        std::cerr << "[GATED_PROBE] deploying buildId=" << buildStatisticId.getRawValue()
                  << " regimeId=" << regimeId.getRawValue() << "\n";
        std::cerr.flush();
        try
        {
            auto probePlan = queryGenerator->generateProbeQuery(
                buildStatisticId, regimeId, *trigger.condition, coordinatorAddress, probeIntervalMs, sinkWorkerHost);
            if (auto submittedProbe = submitPlan(std::move(probePlan)); not submittedProbe.has_value())
            {
                std::cerr << "[GATED_PROBE] submitPlan FAILED for regimeId=" << regimeId.getRawValue() << ": "
                          << submittedProbe.error().what() << "\n";
                std::cerr.flush();
                return std::nullopt;
            }
            std::cerr << "[GATED_PROBE] deployed regimeId=" << regimeId.getRawValue() << "\n";
            std::cerr.flush();
        }
        catch (const std::exception& e)
        {
            std::cerr << "[GATED_PROBE] construction THREW for regimeId=" << regimeId.getRawValue() << ": "
                      << e.what() << "\n";
            std::cerr.flush();
            return std::nullopt;
        }
        if (trigger.callback)
        {
            addProbeCallback(regimeId, trigger.callback);
            std::cerr << "[GATED_PROBE] registered callback for regimeId=" << regimeId.getRawValue() << "\n";
            std::cerr.flush();
        }
        return regimeId;
    };

    if (const auto existing = registry.find(key))
    {
        if (statement.conditionTrigger.has_value())
        {
            /// A second request for the same statistic key. The build branch is already running
            /// under existing->statisticId. We deploy an ADDITIONAL gated probe (with this new
            /// trigger's predicate + a fresh regimeId) so this trigger's callback fires
            /// independently of any earlier triggers' probes.
            if (statement.conditionTrigger->condition.has_value())
            {
                deployGatedProbe(existing->statisticId, *statement.conditionTrigger);
            }
            else
            {
                /// No predicate → fall back to old behavior: append to the registry trigger list,
                /// which fires on the heartbeat probe's ticks.
                registry.addTrigger(key, statement.conditionTrigger.value());
            }
        }
        return CollectStatisticResult{.queryId = existing->queryId, .statisticId = existing->statisticId, .alreadyExisted = true};
    }

    const auto statisticId = Statistic::StatisticId{nextStatisticId.fetch_add(1)};

    /// Stamp DeferSourceStartTrait on the data plan's source so the runtime creates the
    /// RunningSource but doesn't begin emission until the build branch's splice signals start.
    /// Without this gate, the source emits sequences 0..N before the build branch wires in, and
    /// the build branch's MultiOriginWatermarkProcessor gets stuck waiting for those missing
    /// early sequences — windows never close, histogram never populates.
    auto dataPlanWithDeferTrait = dataQueryPlan;
    {
        const auto dataSources = getOperatorByType<SourceNameLogicalOperator>(dataPlanWithDeferTrait);
        if (not dataSources.empty())
        {
            auto taggedSource = LogicalOperator{dataSources.front()};
            auto ts = taggedSource.getTraitSet();
            [[maybe_unused]] const auto inserted = tryInsert(ts, DeferSourceStartTrait{});
            taggedSource = taggedSource.withTraitSet(ts);
            auto replaced = replaceOperator(dataPlanWithDeferTrait, dataSources.front().getId(), taggedSource);
            if (replaced.has_value())
            {
                dataPlanWithDeferTrait = std::move(*replaced);
            }
        }
    }

    /// Deploy the (tagged) data plan. The statistic build branch is submitted as a *separate*
    /// query below; its source carries SpliceToRunningSourceTrait so at runtime instantiation
    /// the build branch splices into the data query's already-registered (but not yet emitting)
    /// running source pipeline. The splice itself triggers startEmitting() so emission starts at
    /// sequence 0 with the build branch already attached.
    auto submittedData = submitPlan(std::move(dataPlanWithDeferTrait));
    if (not submittedData.has_value())
    {
        return std::unexpected(submittedData.error());
    }
    const auto mergedQueryId = std::move(submittedData.value());

    /// Submit the build branch as its own query. Its source carries SpliceToRunningSourceTrait,
    /// so on the worker side ExecutableQueryPlan::instantiate will redirect it to the data
    /// query's running source instead of spawning a new source thread. Failure here is logged
    /// but does not undo the data-query deploy — the build branch is an observability concern.
    try
    {
        auto buildBranch = queryGenerator->generateWorkloadBranch(*domain, statement, statisticId, coordinatorAddress, spliceLeaf);
        if (auto submittedBranch = submitPlan(std::move(buildBranch)); not submittedBranch.has_value())
        {
            NES_WARNING(
                "Workload-domain build branch deploy failed (statisticId={}): {}",
                statisticId.getRawValue(),
                submittedBranch.error().what());
        }
    }
    catch (const std::exception& e)
    {
        NES_WARNING("Workload-domain build branch construction threw (statisticId={}): {}", statisticId.getRawValue(), e.what());
    }

    /// Probe deployment:
    ///  - With a predicate: deploy a gated pipeline that only reports when the histogram's bins
    ///    match `condition`. The trigger's callback is registered under the fresh regimeId, not
    ///    the build branch's statisticId. The registry's `triggers` list is left empty so the
    ///    fallback heartbeat path doesn't double-fire the callback.
    ///  - Without a predicate: legacy heartbeat — probe pings every interval and the registry's
    ///    `triggers` list fires the callback unconditionally.
    std::vector<ConditionTrigger> triggers;
    const bool hasGatedTrigger
        = statement.conditionTrigger.has_value() and statement.conditionTrigger->condition.has_value();

    if (hasGatedTrigger)
    {
        deployGatedProbe(statisticId, *statement.conditionTrigger);
    }
    else
    {
        try
        {
            auto probePlan = queryGenerator->generateProbeQuery(
                statisticId, statisticId, std::nullopt, coordinatorAddress, probeIntervalMs, sinkWorkerHost);
            if (auto submittedProbe = submitPlan(std::move(probePlan)); not submittedProbe.has_value())
            {
                NES_WARNING(
                    "Workload-domain probe deploy failed (statisticId={}): {}",
                    statisticId.getRawValue(),
                    submittedProbe.error().what());
            }
        }
        catch (const std::exception& e)
        {
            NES_WARNING(
                "Workload-domain probe construction threw (statisticId={}): {}", statisticId.getRawValue(), e.what());
        }
        if (statement.conditionTrigger.has_value())
        {
            triggers.emplace_back(*statement.conditionTrigger);
        }
    }
    registry.registerStatistic(key, mergedQueryId, statisticId, std::move(triggers));
    return CollectStatisticResult{.queryId = mergedQueryId, .statisticId = statisticId, .alreadyExisted = false};
}

bool StatisticCoordinator::addConditionTrigger(const StatisticRegistry::Key& key, ConditionTrigger trigger)
{
    return registry.addTrigger(key, std::move(trigger));
}

bool StatisticCoordinator::deregisterStatistic(const StatisticRegistry::Key& key)
{
    return registry.deregisterStatistic(key);
}

std::string StatisticCoordinator::startGrpcServer()
{
    auto service = std::make_unique<StatisticCoordinatorServiceImpl>(*this);
    grpc::ServerBuilder builder;
    int selectedPort = 0;
    builder.AddListeningPort(addressAndPort, grpc::InsecureServerCredentials(), &selectedPort);
    builder.RegisterService(service.get());
    grpcServer = builder.BuildAndStart();
    if (not grpcServer)
    {
        throw GRPCError("StatisticCoordinator: Failed to start gRPC server");
    }
    service.release(); /// NOLINT(bugprone-unused-return-value)
    coordinatorAddress = "localhost:" + std::to_string(selectedPort);
    NES_INFO("StatisticCoordinator gRPC server listening on {}", coordinatorAddress);
    return coordinatorAddress;
}

void StatisticCoordinator::stopGrpcServer()
{
    if (grpcServer)
    {
        grpcServer->Shutdown();
        grpcServer.reset();
        NES_DEBUG("StatisticCoordinator gRPC server stopped.");
    }
}

std::optional<double> StatisticCoordinator::getStatistics(
    const std::vector<StatisticRegistry::Key>& keys,
    Windowing::TimeMeasure startTs,
    Windowing::TimeMeasure endTs,
    LogicalPlan& probeQueryWithoutSource)
{
    /// Look up statisticIds for all keys.
    std::vector<Statistic::StatisticId> statisticIds;
    for (const auto& key : keys)
    {
        auto entry = registry.find(key);
        if (not entry.has_value())
        {
            throw QueryNotFound("StatisticCoordinator::getStatistics: key not found in registry");
        }
        statisticIds.push_back(entry->statisticId);
    }

    /// Parse coordinator address into host:port for the sink config.
    const auto colonPos = coordinatorAddress.find(':');
    const auto sinkHost = coordinatorAddress.substr(0, colonPos);
    const auto sinkPort = coordinatorAddress.substr(colonPos + 1);

    /// Try to submit the probe query with different gRPC source ports until one succeeds.
    constexpr uint32_t startPort = 10000;
    constexpr uint32_t maxRetries = 10;
    uint32_t grpcSourcePort = 0;
    auto probeQueryId = QueryId::invalid();

    for (uint32_t attempt = 0; attempt < maxRetries; attempt++)
    {
        grpcSourcePort = startPort + attempt;

        /// Build the full probe query: GrpcSource → probeQueryWithoutSource → GrpcSink
        Schema grpcSourceSchema;
        const LogicalStatisticFields statisticFields;
        grpcSourceSchema.addField(statisticFields.statisticIdField);
        grpcSourceSchema.addField(statisticFields.statisticStartTsField);
        grpcSourceSchema.addField(statisticFields.statisticEndTsField);

        auto plan = LogicalPlanBuilder::createLogicalPlan(
            "Grpc", grpcSourceSchema, {{"grpc_port", std::to_string(grpcSourcePort)}, {"receive_timeout_ms", "5000"}}, {});

        for (const auto& rootOp : probeQueryWithoutSource.getRootOperators())
        {
            plan = LogicalPlanBuilder::addStatProbeOp(rootOp, plan);
        }

        Schema grpcSinkSchema;
        grpcSinkSchema.addField(statisticFields.statisticIdField);
        grpcSinkSchema.addField(statisticFields.statisticStartTsField);
        grpcSinkSchema.addField(statisticFields.statisticEndTsField);

        plan = LogicalPlanBuilder::addInlineSink("Grpc", grpcSinkSchema, {{"grpc_host", sinkHost}, {"grpc_port", sinkPort}}, {}, plan);

        auto queryIdResult = submitQuery(std::move(plan));
        if (queryIdResult.has_value())
        {
            probeQueryId = queryIdResult.value();
            NES_DEBUG(
                "StatisticCoordinator::getStatistics: probe query submitted as queryId={} with gRPC source port {}",
                probeQueryId,
                grpcSourcePort);
            break;
        }

        NES_WARNING(
            "StatisticCoordinator::getStatistics: failed to submit probe query on port {}: {}",
            grpcSourcePort,
            queryIdResult.error().what());

        if (attempt == maxRetries - 1)
        {
            throw QueryStartFailed("StatisticCoordinator::getStatistics: failed to submit probe query after {} attempts", maxRetries);
        }
    }

    /// Register pending probes so we can wait for results.
    std::vector<std::future<double>> futures;
    for (const auto& statId : statisticIds)
    {
        auto [future, promise] = []
        {
            std::promise<double> p;
            auto f = p.get_future();
            return std::pair{std::move(f), std::move(p)};
        }();
        pendingProbes.wlock()->emplace(statId, PendingProbe{.promise = std::move(promise)});
        futures.push_back(std::move(future));
    }

    /// Send StatisticRequests to the gRPC source to trigger the probe.
    /// The source runs on localhost at the port we configured above.
    const auto sourceAddress = "localhost:" + std::to_string(grpcSourcePort);
    auto channel = grpc::CreateChannel(sourceAddress, grpc::InsecureChannelCredentials());
    auto sourceStub = StatisticSourceService::NewStub(channel);

    for (const auto& statId : statisticIds)
    {
        StatisticRequest request;
        request.set_statistic_id(statId.getRawValue());
        request.set_start_ts(startTs.getTime());
        request.set_end_ts(endTs.getTime());

        grpc::ClientContext context;
        google::protobuf::Empty response;
        auto status = sourceStub->RequestStatistic(&context, request, &response);
        if (not status.ok())
        {
            NES_WARNING(
                "StatisticCoordinator::getStatistics: RequestStatistic failed for statisticId={}: {}", statId, status.error_message());
        }
    }

    /// Wait for results with a timeout.
    constexpr auto timeout = std::chrono::seconds{30};
    double result = 0.0;
    bool allReceived = true;
    for (auto& future : futures)
    {
        if (future.wait_for(timeout) == std::future_status::ready)
        {
            result += future.get();
        }
        else
        {
            NES_WARNING("StatisticCoordinator::getStatistics: timeout waiting for probe result");
            allReceived = false;
        }
    }

    /// Clean up pending probes.
    for (const auto& statId : statisticIds)
    {
        pendingProbes.wlock()->erase(statId);
    }

    if (not allReceived)
    {
        return {};
    }
    return result;
}

void StatisticCoordinator::onStatisticReport(
    const Statistic::StatisticId statisticId, const Windowing::TimeMeasure startTs, const Windowing::TimeMeasure endTs, const double value)
{
    /// Check if this is a response to a pending probe query.
    {
        auto probes = pendingProbes.wlock();
        if (auto it = probes->find(statisticId); it != probes->end())
        {
            it->second.promise.set_value(value);
            probes->erase(it);
            return;
        }
    }

    /// Selectivity-gated probe path: regime-specific statisticIds are routed directly via the
    /// probeCallbacks map. The registry scan below wouldn't match anyway (regime ids are not
    /// stored as entry.statisticId) so we return after firing.
    {
        std::vector<ProbeCallback> snapshot;
        {
            auto callbacks = probeCallbacks.rlock();
            if (auto it = callbacks->find(statisticId); it != callbacks->end())
            {
                snapshot = it->second;
            }
        }
        if (not snapshot.empty())
        {
            for (const auto& cb : snapshot)
            {
                cb(statisticId, startTs, endTs);
            }
            return;
        }
    }

    /// Not a probe response — check for condition triggers in the registry.
    /// We iterate over all entries to find one matching this statisticId.
    /// This is acceptable for now since the registry is typically small.
    registry.forEachEntry(
        [&](const auto&, const StatisticRegistry::Entry& entry)
        {
            if (entry.statisticId == statisticId)
            {
                for (const auto& [_, callback] : entry.triggers)
                {
                    callback(statisticId, startTs, endTs);
                }
            }
        });
}

void StatisticCoordinator::addProbeCallback(Statistic::StatisticId probeStatisticId, ProbeCallback callback)
{
    auto callbacks = probeCallbacks.wlock();
    (*callbacks)[probeStatisticId].push_back(std::move(callback));
}

}
