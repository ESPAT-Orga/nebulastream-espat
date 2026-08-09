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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Traits/DeferSourceStartTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Strings.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/ranges.h>
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
const auto* const addressAndPort = "0.0.0.0:0";

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
    const std::function<std::expected<QueryId, Exception>(LogicalPlan)>& submitPlan)
{
    const auto* domain = std::get_if<WorkloadDomain>(&statement.domain);
    if (domain == nullptr)
    {
        return std::unexpected(InvalidConfigParameter("collectWorkloadStatistic requires a WorkloadDomain in the request"));
    }

    /// Pick the SourceNameLogicalOperator to splice the build branch onto, as a sibling consumer of
    /// that operator.
    ///
    /// Single-source data queries (the adaptive-optimization case: one Memory source feeding a
    /// filter chain) need no disambiguation. Join-shaped data queries have several sources and each
    /// companion must say which one it observes — via the `splice_source` option, naming the logical
    /// source. The field name cannot serve as the discriminator: in Nexmark Q8 both `person` and
    /// `auction` carry an `id`, so `--companion-field id` would be ambiguous exactly where it
    /// matters. domain.operatorId would work too, but the REPL companion path leaves it
    /// INVALID_OPERATOR_ID, so the caller has no way to set it.
    const auto sources = getOperatorByType<SourceNameLogicalOperator>(dataQueryPlan);
    if (sources.empty())
    {
        return std::unexpected(InvalidConfigParameter("WorkloadDomain splice: data query has no source-name operator"));
    }

    auto sourceNamesForDiagnostics = [&sources]
    {
        std::vector<std::string> names;
        names.reserve(sources.size());
        for (const auto& source : sources)
        {
            names.emplace_back(source->getLogicalSourceName());
        }
        return fmt::format("{}", fmt::join(names, ", "));
    };

    size_t spliceIndex = 0;
    if (const auto requestedIt = statement.options.find("splice_source");
        requestedIt != statement.options.end() and not requestedIt->second.empty())
    {
        const auto requested = toUpperCase(requestedIt->second);
        const auto match = std::ranges::find_if(
            sources, [&requested](const auto& source) { return toUpperCase(source->getLogicalSourceName()) == requested; });
        if (match == sources.end())
        {
            return std::unexpected(InvalidConfigParameter(
                "WorkloadDomain splice: data query has no source named '{}' (candidates: {})",
                requestedIt->second,
                sourceNamesForDiagnostics()));
        }
        spliceIndex = static_cast<size_t>(std::ranges::distance(sources.begin(), match));
    }
    else if (sources.size() != 1)
    {
        return std::unexpected(InvalidConfigParameter(
            "WorkloadDomain splice: data query has {} sources ({}), so the request must name one via the "
            "'splice_source' option",
            sources.size(),
            sourceNamesForDiagnostics()));
    }
    const LogicalOperator spliceLeaf{sources[spliceIndex]};
    const auto sourceNameUpper = sources[spliceIndex]->getLogicalSourceName();

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

    const auto statisticId = Statistic::StatisticId{nextStatisticId.fetch_add(1)};

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

    /// Per-source splice counts for join-shaped data queries, as "PERSON=1,AUCTION=1" in the
    /// `splice_counts` option. Each source must defer on the number of build branches that will
    /// attach to *it*, not on the session-wide total: with one companion per source of a two-source
    /// join, stamping the session total (2) on one source would make it wait for a splice that
    /// never lands there. Absent the option we keep the single-source behaviour exactly — stamp the
    /// first source with `expected_splice_count`.
    std::unordered_map<std::string, uint32_t> perSourceSpliceCounts;
    if (const auto countsIt = statement.options.find("splice_counts"); countsIt != statement.options.end() and not countsIt->second.empty())
    {
        const std::string_view spec{countsIt->second};
        for (size_t begin = 0; begin < spec.size();)
        {
            const auto end = std::min(spec.find(',', begin), spec.size());
            const auto pair = spec.substr(begin, end - begin);
            begin = end + 1;
            const auto eq = pair.find('=');
            if (eq == std::string_view::npos)
            {
                continue;
            }
            CPPTRACE_TRY
            {
                perSourceSpliceCounts.insert_or_assign(
                    toUpperCase(pair.substr(0, eq)),
                    std::max<uint32_t>(1, static_cast<uint32_t>(std::stoul(std::string{pair.substr(eq + 1)}))));
            }
            CPPTRACE_CATCH(...)
            {
                /// skip an unparseable entry rather than failing the whole deploy
            }
        }
    }

    auto dataPlanWithDeferTrait = dataQueryPlan;
    {
        const auto dataSources = getOperatorByType<SourceNameLogicalOperator>(dataPlanWithDeferTrait);
        for (size_t i = 0; i < dataSources.size(); ++i)
        {
            uint32_t countForSource = 0;
            if (perSourceSpliceCounts.empty())
            {
                /// Single-source path: only the first source defers, on the session-wide count.
                countForSource = (i == 0) ? expectedSpliceCount : 0;
            }
            else if (const auto it = perSourceSpliceCounts.find(toUpperCase(dataSources[i]->getLogicalSourceName()));
                     it != perSourceSpliceCounts.end())
            {
                countForSource = it->second;
            }
            if (countForSource == 0)
            {
                /// No build branch attaches here, so nothing to wait for.
                continue;
            }
            auto taggedSource = LogicalOperator{dataSources[i]};
            auto ts = taggedSource.getTraitSet();
            [[maybe_unused]] const auto inserted = tryInsert(ts, DeferSourceStartTrait{.expectedSpliceCount = countForSource});
            taggedSource = taggedSource.withTraitSet(ts);
            auto replaced = replaceOperator(dataPlanWithDeferTrait, dataSources[i].getId(), taggedSource);
            if (replaced.has_value())
            {
                dataPlanWithDeferTrait = std::move(*replaced);
            }
        }
    }

    /// Deploy the (tagged) data plan once, however many companions attach to it. Each companion
    /// request has its own WorkloadDomain → its own registry key → all of them reach this "new"
    /// path, so without a cache we would deploy the data query once per companion.
    ///
    /// The cache is keyed by the data plan's root operator ids, not by logical source name. Source
    /// name was wrong in two ways for join-shaped queries: a companion on `auction` would miss the
    /// entry left by the one on `person` and deploy the join a *second* time, and two different
    /// data queries reading the same source would collide and the second would never deploy at all.
    /// Repl.cpp passes the identical LogicalPlan for every companion of one query, so the root ids
    /// identify it exactly.
    const auto dataPlanKey = [&dataQueryPlan]
    {
        std::vector<std::string> rootIds;
        for (const auto& root : dataQueryPlan.getRootOperators())
        {
            rootIds.emplace_back(std::to_string(root.getId().getRawValue()));
        }
        std::ranges::sort(rootIds);
        return fmt::format("{}", fmt::join(rootIds, "-"));
    }();

    std::optional<QueryId> mergedQueryIdOpt;
    bool deployedHere = false;
    {
        auto cache = deployedDataQueriesByPlan.wlock();
        if (const auto it = cache->find(dataPlanKey); it != cache->end())
        {
            mergedQueryIdOpt = it->second;
        }
        else
        {
            deployedHere = true;
            auto submittedData = submitPlan(std::move(dataPlanWithDeferTrait));
            if (not submittedData.has_value())
            {
                return std::unexpected(submittedData.error());
            }
            mergedQueryIdOpt = std::move(submittedData.value());
            cache->emplace(dataPlanKey, *mergedQueryIdOpt);
        }
    }
    const auto mergedQueryId = *mergedQueryIdOpt;

    /// DIAGNOSTIC, gated on NES_SPLICE_DEPLOY_DELAY_MS (unset = no delay, production behaviour).
    ///
    /// Companions are collected one request at a time, so the FIRST one deploys the data plan and
    /// then submits its build branch immediately, while every later companion finds the plan cached
    /// and submits into a worker that has had time to instantiate the data query. Observed effect:
    /// the first companion's branch never splices and its source waits on DeferSourceStartTrait
    /// forever. If that head start is the cause, pausing here should make the first splice land.
    ///
    /// If this confirms the race, the real fix is to deploy the data plan once up front and then
    /// submit all branches, rather than deploying inside the first request's call.
    if (deployedHere)
    {
        if (const auto* delayMs = std::getenv("NES_SPLICE_DEPLOY_DELAY_MS"))
        {
            CPPTRACE_TRY
            {
                const auto millis = std::stoul(delayMs);
                NES_WARNING("collectWorkloadStatistic: NES_SPLICE_DEPLOY_DELAY_MS={} — pausing after data-plan deploy", millis);
                std::this_thread::sleep_for(std::chrono::milliseconds{millis});
            }
            CPPTRACE_CATCH(...)
            {
                /// unparseable value: no delay
            }
        }
    }

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
