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

#include <StatisticManager.hpp>

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <ranges>
#include <vector>
#include <Schema/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Traits/DeferSourceStartTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <cpptrace/from_current.hpp>
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
#include <StatisticTuple.hpp>
#include <StatisticQueryGenerator.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>

namespace NES
{
const auto* const addressAndPort = "0.0.0.0:0";

/// gRPC service implementation that routes incoming reports to the StatisticManager.
class StatisticManagerServiceImpl final : public StatisticManagerService::Service
{
public:
    explicit StatisticManagerServiceImpl(StatisticManager& coordinator) : coordinator(coordinator) { }

    grpc::Status ReportStatistic(grpc::ServerContext*, const StatisticReport* report, google::protobuf::Empty*) override
    {
        coordinator.onStatisticReport(
            StatisticTuple::StatisticId{report->statistic_id()},
            Windowing::TimeMeasure{report->start_ts()},
            Windowing::TimeMeasure{report->end_ts()},
            report->value());
        return grpc::Status::OK;
    }

private:
    StatisticManager& coordinator;
};

StatisticManager::StatisticManager(std::unique_ptr<StatisticQueryGenerator> queryGenerator, SubmitQueryFn submitQuery)
    : queryGenerator(std::move(queryGenerator)), submitQuery(std::move(submitQuery))
{
}

StatisticManager::StatisticManager(StatisticManager&& other) noexcept
    : nextStatisticId(other.nextStatisticId.load())
    , registry(std::move(other.registry))
    , queryGenerator(std::move(other.queryGenerator))
    , submitQuery(std::move(other.submitQuery))
    , grpcServer(std::move(other.grpcServer))
    , coordinatorAddress(std::move(other.coordinatorAddress))
{
}

StatisticManager& StatisticManager::operator=(StatisticManager&& other) noexcept
{
    nextStatisticId.store(other.nextStatisticId.load());
    registry = std::move(other.registry);
    queryGenerator = std::move(other.queryGenerator);
    submitQuery = std::move(other.submitQuery);
    grpcServer = std::move(other.grpcServer);
    coordinatorAddress = std::move(other.coordinatorAddress);
    return *this;
}

StatisticManager::~StatisticManager()
{
    stopGrpcServer();
}

std::expected<CollectStatisticResult, Exception> StatisticManager::collectNewStatistic(const RequestStatisticBuildStatement& statement)
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

    const auto statisticId = StatisticTuple::StatisticId{nextStatisticId.fetch_add(1)};
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

std::expected<CollectStatisticResult, Exception> StatisticManager::collectWorkloadStatistic(
    const RequestStatisticBuildStatement& statement,
    const LogicalPlan& dataQueryPlan,
    const std::function<std::expected<QueryId, Exception>(LogicalPlan)>& submitPlan)
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
        return std::unexpected(
            NotImplemented("WorkloadDomain splice MVP requires the data query to have exactly one source (got {})", sources.size()));
    }
    const LogicalOperator spliceLeaf{sources.front()};
    const auto sourceNameUpper = sources.front()->getLogicalSourceName();

    const StatisticRegistry::Key key{
        .metric = statement.metric, .collectionDomain = statement.domain, .windowSize = Windowing::TimeMeasure{statement.windowSizeMs}};

    const auto hostIt = statement.options.find("host");
    const auto& sinkWorkerHost = hostIt != statement.options.end() ? hostIt->second : std::string{"localhost:8080"};
    (void)sinkWorkerHost; /// kept for forward-compat with future per-request host overrides

    if (const auto existing = registry.find(key))
    {
        if (statement.conditionTrigger.has_value() and statement.conditionTrigger->callback)
        {
            /// Same registry key (same metric, domain, window): the build branch is already
            /// running under existing->statisticId. We only need to register an additional
            /// callback. The existing in-build probe will route to ALL callbacks registered
            /// under this statisticId. (Multiple predicates on the same field would require
            /// per-callback predicates instead — currently the predicate is baked into the
            /// in-build Selection at deploy time.)
            addProbeCallback(existing->statisticId, statement.conditionTrigger->callback);
        }
        return CollectStatisticResult{.queryId = existing->queryId, .statisticId = existing->statisticId, .alreadyExisted = true};
    }

    const auto statisticId = StatisticTuple::StatisticId{nextStatisticId.fetch_add(1)};

    /// Stamp DeferSourceStartTrait on the data plan's source so the runtime creates the
    /// RunningSource but doesn't begin emission until ALL expected splices have wired in.
    /// `expected_splice_count` in the request options carries the count (set by the REPL based
    /// on how many companion-statistic requests it intends to deploy in this session). Default
    /// 1 (single splice case) when the caller doesn't specify.
    uint32_t expectedSpliceCount = 1;
    if (const auto countIt = statement.options.find("expected_splice_count"); countIt != statement.options.end())
    {
        CPPTRACE_TRY
        {
            expectedSpliceCount = std::max<uint32_t>(1, static_cast<uint32_t>(std::stoul(countIt->second)));
        }
        CPPTRACE_CATCH(...)
        {
            /// keep default 1
        }
    }
    auto dataPlanWithDeferTrait = dataQueryPlan;
    {
        const auto dataSources = getOperatorByType<SourceNameLogicalOperator>(dataPlanWithDeferTrait);
        if (not dataSources.empty())
        {
            auto taggedSource = LogicalOperator{dataSources.front()};
            auto ts = taggedSource.getTraitSet();
            [[maybe_unused]] const auto inserted = tryInsert(ts, DeferSourceStartTrait{.expectedSpliceCount = expectedSpliceCount});
            taggedSource = taggedSource.withTraitSet(ts);
            /// statistic-renaming calls PlanRewriteUtils::replaceOperator here, which upstream does not have, so
            /// we walk the plan ourselves and swap the one operator by id.
            const auto targetId = dataSources.front().getId();
            const std::function<LogicalOperator(const LogicalOperator&)> replaceInTree
                = [&](const LogicalOperator& op) -> LogicalOperator
            {
                if (op.getId() == targetId)
                {
                    return taggedSource;
                }
                auto children = op.getChildren();
                std::vector<LogicalOperator> newChildren;
                newChildren.reserve(children.size());
                for (const auto& child : children)
                {
                    newChildren.push_back(replaceInTree(child));
                }
                return op.withChildrenUnsafe(std::move(newChildren));
            };
            std::vector<LogicalOperator> newRoots;
            for (const auto& root : dataPlanWithDeferTrait.getRootOperators())
            {
                newRoots.push_back(replaceInTree(root));
            }
            dataPlanWithDeferTrait = dataPlanWithDeferTrait.withRootOperators(newRoots);
        }
    }

    /// Deploy the (tagged) data plan if no data query for this logical source has been deployed
    /// yet. With multiple companion-statistic requests covering different fields of the same
    /// source (each with its own WorkloadDomain → its own registry key → both going through this
    /// "new" path), we must NOT deploy a duplicate data query. The cache keyed by logical source
    /// name catches that and reuses the existing queryId.
    std::optional<QueryId> mergedQueryIdOpt;
    {
        auto cache = deployedDataQueriesBySource.wlock();
        if (const auto it = cache->find(sourceNameUpper.asCanonicalString()); it != cache->end())
        {
            mergedQueryIdOpt = it->second;
        }
        else
        {
            auto submittedData = submitPlan(std::move(dataPlanWithDeferTrait));
            if (not submittedData.has_value())
            {
                return std::unexpected(submittedData.error());
            }
            mergedQueryIdOpt = std::move(submittedData.value());
            cache->emplace(sourceNameUpper.asCanonicalString(), *mergedQueryIdOpt);
        }
    }
    const auto mergedQueryId = *mergedQueryIdOpt;

    /// Submit the build branch as its own query. Its source carries SpliceToRunningSourceTrait,
    /// so on the worker side ExecutableQueryPlan::instantiate will redirect it to the data
    /// query's running source instead of spawning a new source thread. Failure here is logged
    /// but does not undo the data-query deploy — the build branch is an observability concern.
    /// Prometheus-baseline mode: when the request carries a (non-empty) prometheus_server_url, the
    /// build branch routes the monitored field into a PrometheusSink instead of the in-engine
    /// StatisticBuild/StoreWriter/Probe→GrpcSink chain. No probe reports come back over gRPC in
    /// this mode (the external Prometheus scrapes the sink and the coordinator polls Prometheus),
    /// so no probe callback is registered below.
    const auto prometheusUrlIt = statement.options.find("prometheus_server_url");
    const bool prometheusBaseline = prometheusUrlIt != statement.options.end() and not prometheusUrlIt->second.empty();

    try
    {
        auto buildBranch = prometheusBaseline
            ? queryGenerator->generateWorkloadBranchPrometheus(*domain, statement, spliceLeaf)
            : queryGenerator->generateWorkloadBranch(*domain, statement, statisticId, coordinatorAddress, spliceLeaf);
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

    /// Register the trigger's callback under the build statisticId. The build branch's in-line
    /// probe (Probe → Selection → Projection → GrpcSink) reports to the coordinator on each
    /// window-close that survives the Selection predicate, and the report carries this
    /// statisticId so probeCallbacks[statisticId] is the right routing key. Skipped in
    /// Prometheus-baseline mode, where no gRPC reports are produced.
    if (not prometheusBaseline and statement.conditionTrigger.has_value() and statement.conditionTrigger->callback)
    {
        addProbeCallback(statisticId, statement.conditionTrigger->callback);
    }
    registry.registerStatistic(key, mergedQueryId, statisticId, /*triggers=*/{});
    return CollectStatisticResult{.queryId = mergedQueryId, .statisticId = statisticId, .alreadyExisted = false};
}

bool StatisticManager::addConditionTrigger(const StatisticRegistry::Key& key, ConditionTrigger trigger)
{
    return registry.addTrigger(key, std::move(trigger));
}

bool StatisticManager::deregisterStatistic(const StatisticRegistry::Key& key)
{
    return registry.deregisterStatistic(key);
}

std::string StatisticManager::startGrpcServer()
{
    auto service = std::make_unique<StatisticManagerServiceImpl>(*this);
    grpc::ServerBuilder builder;
    int selectedPort = 0;
    builder.AddListeningPort(addressAndPort, grpc::InsecureServerCredentials(), &selectedPort);
    builder.RegisterService(service.get());
    grpcServer = builder.BuildAndStart();
    if (not grpcServer)
    {
        throw QueryStartFailed("StatisticManager: Failed to start gRPC server");
    }
    service.release(); /// NOLINT(bugprone-unused-return-value)
    coordinatorAddress = "localhost:" + std::to_string(selectedPort);
    NES_INFO("StatisticManager gRPC server listening on {}", coordinatorAddress);
    return coordinatorAddress;
}

void StatisticManager::stopGrpcServer()
{
    if (grpcServer)
    {
        grpcServer->Shutdown();
        grpcServer.reset();
        NES_DEBUG("StatisticManager gRPC server stopped.");
    }
}

std::optional<double> StatisticManager::getStatistics(
    const std::vector<StatisticRegistry::Key>& keys,
    Windowing::TimeMeasure startTs,
    Windowing::TimeMeasure endTs,
    LogicalPlan& probeQueryWithoutSource)
{
    /// Look up statisticIds for all keys.
    std::vector<StatisticTuple::StatisticId> statisticIds;
    for (const auto& key : keys)
    {
        auto entry = registry.find(key);
        if (not entry.has_value())
        {
            throw QueryNotFound("StatisticManager::getStatistics: key not found in registry");
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

        /// Build the full probe query: GrpcSource → probeQueryWithoutSource → GrpcSink.
        /// Upstream's Schema is templated and declared schemas use unbound fields, so the statistic field
        /// constants are materialised via StatisticField::unbound() rather than added one by one.
        const LogicalStatisticFields statisticFields;
        const auto statisticKeySchema = std::vector<UnqualifiedUnboundField>{
                                            statisticFields.statisticIdField.unbound(),
                                            statisticFields.statisticStartTsField.unbound(),
                                            statisticFields.statisticEndTsField.unbound()}
            | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();

        auto plan = LogicalPlanBuilder::createLogicalPlan(
            Identifier::parse("Grpc"),
            statisticKeySchema,
            {{Identifier::parse("grpc_port"), std::to_string(grpcSourcePort)}, {Identifier::parse("receive_timeout_ms"), "5000"}},
            {});

        for (const auto& rootOp : probeQueryWithoutSource.getRootOperators())
        {
            plan = LogicalPlanBuilder::addStatProbeOp(rootOp, plan);
        }

        plan = LogicalPlanBuilder::addAnonymousSink(
            Identifier::parse("Grpc"),
            statisticKeySchema,
            {{Identifier::parse("grpc_host"), sinkHost}, {Identifier::parse("grpc_port"), sinkPort}},
            {},
            plan);

        auto queryIdResult = submitQuery(std::move(plan));
        if (queryIdResult.has_value())
        {
            probeQueryId = queryIdResult.value();
            NES_DEBUG(
                "StatisticManager::getStatistics: probe query submitted as queryId={} with gRPC source port {}",
                probeQueryId,
                grpcSourcePort);
            break;
        }

        NES_WARNING(
            "StatisticManager::getStatistics: failed to submit probe query on port {}: {}",
            grpcSourcePort,
            queryIdResult.error().what());

        if (attempt == maxRetries - 1)
        {
            throw QueryStartFailed("StatisticManager::getStatistics: failed to submit probe query after {} attempts", maxRetries);
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
                "StatisticManager::getStatistics: RequestStatistic failed for statisticId={}: {}", statId, status.error_message());
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
            NES_WARNING("StatisticManager::getStatistics: timeout waiting for probe result");
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

void StatisticManager::onStatisticReport(
    const StatisticTuple::StatisticId statisticId, const Windowing::TimeMeasure startTs, const Windowing::TimeMeasure endTs, const double value)
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

void StatisticManager::addProbeCallback(StatisticTuple::StatisticId probeStatisticId, ProbeCallback callback)
{
    auto callbacks = probeCallbacks.wlock();
    (*callbacks)[probeStatisticId].push_back(std::move(callback));
}

}
