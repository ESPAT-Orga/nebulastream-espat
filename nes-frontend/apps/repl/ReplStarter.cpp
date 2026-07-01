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

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <ranges>
#include <stop_token>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>

#include <Identifiers/Identifiers.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/JsonOutputFormatter.hpp>
#include <Statements/StatementHandler.hpp>
#include <Statements/StatementOutputAssembler.hpp>
#include <Statements/TextOutputFormatter.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Signal.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <magic_enum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <DefaultStatisticQueryGenerator.hpp>
#include <ErrorHandling.hpp>
#include <PrometheusQuery.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <Repl.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SingleNodeWorkerRPCService.pb.h>
#include <StatisticCoordinator.hpp>
#include <Thread.hpp>
#include <WorkerCatalog.hpp>
#include <utils.hpp>

#ifdef EMBED_ENGINE
    #include <Configurations/Util.hpp>
    #include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
    #include <SingleNodeWorkerConfiguration.hpp>
    #include <WorkerConfig.hpp>
#endif

/// If repl is executed with an embedded worker, this switch prevents actual port allocation and routes all inter-worker communication
/// via an in-memory channel.

extern void enable_memcom();

namespace
{
/// Shared worker-facing action for BOTH adaptive-redeployment paths: the native gated probe
/// callback and the Prometheus-baseline poll loop. Encapsulates the gRPC SetSwitch call plus the
/// gate-state guard so a switch is issued only on an actual regime change, never once-per-tick.
/// Lives in the coordinator process (= this REPL/frontend process); both paths flip the worker's
/// filter-order gate through this one object so the comparison differs only in how the regime is
/// detected, not in how the switch is applied.
class WorkloadSwitchClient
{
public:
    explicit WorkloadSwitchClient(std::string workerServerUri) : workerServerUri(std::move(workerServerUri)) { }

    /// Set the named gate to `targetValue` on the worker. No-op (returns false) if the gate is
    /// already at that value — an unknown gate is treated as its initial value 0 (the data sink is
    /// gated expected=0). On gRPC failure the cached state is NOT advanced, so the next call retries.
    /// Returns true iff a SetSwitch was actually applied. The whole call is serialized under `mutex`,
    /// which also makes concurrent native-callback fires safe (switches are rare, so contention is nil).
    bool setSwitch(const std::string& switchName, int64_t targetValue)
    {
        const std::lock_guard lock(mutex);
        const auto it = currentValues.find(switchName);
        const int64_t current = it != currentValues.end() ? it->second : 0;
        if (current == targetValue)
        {
            return false;
        }
        auto stub = WorkerRPCService::NewStub(grpc::CreateChannel(workerServerUri, grpc::InsecureChannelCredentials()));
        grpc::ClientContext ctx;
        SetSwitchRequest req;
        req.set_name(switchName);
        req.set_value(targetValue);
        google::protobuf::Empty resp;
        if (const auto status = stub->SetSwitch(&ctx, req, &resp); not status.ok())
        {
            NES_WARNING("SetSwitch({}={}) failed: {}", switchName, targetValue, status.error_message());
            return false;
        }
        currentValues[switchName] = targetValue;
        NES_INFO("WorkloadSwitch: set gate '{}' -> {}", switchName, targetValue);
        return true;
    }

private:
    std::string workerServerUri;
    std::mutex mutex;
    std::unordered_map<std::string, int64_t> currentValues;
};

enum class OnExitBehavior : uint8_t
{
    WAIT_FOR_QUERY_TERMINATION,
    STOP_QUERIES,
    DO_NOTHING,
};

class SignalHandler
{
    static inline std::stop_source signalSource;

public:
    static void setup()
    {
        const auto previousHandler = std::signal(SIGTERM, [](int) { [[maybe_unused]] auto dontCare = signalSource.request_stop(); });
        if (previousHandler == SIG_ERR)
        {
            NES_WARNING("Could not install signal handler for SIGTERM. Repl might not respond to termination signals.");
        }
        else
        {
            INVARIANT(
                previousHandler == nullptr,
                "The SignalHandler does not restore the pre existing signal handler and thus it expects no handler to exist");
        }
    }

    static std::stop_token terminationToken() { return signalSource.get_token(); }
};

std::ostream& printStatementResult(std::ostream& os, NES::StatementOutputFormat format, const auto& statement)
{
    NES::StatementOutputAssembler<std::remove_cvref_t<decltype(statement)>> assembler{};
    auto result = assembler.convert(statement);
    switch (format)
    {
        case NES::StatementOutputFormat::TEXT:
            return os << toText(result);
        case NES::StatementOutputFormat::JSON:
            return os << nlohmann::json(result).dump() << '\n';
    }
    std::unreachable();
}
}

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::setupSignalHandlers();
        bool interactiveMode
            = static_cast<int>(cpptrace::isatty(STDIN_FILENO)) != 0 and static_cast<int>(cpptrace::isatty(STDOUT_FILENO)) != 0;

        NES::Thread::initializeThread(NES::Host("nes-repl"), "main");
        NES::Logger::setupLogging("nes-repl.log", NES::LogLevel::LOG_ERROR, false);
        SignalHandler::setup();

        using argparse::ArgumentParser;
        ArgumentParser program("nes-repl");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-s", "--server").help("Server URI to connect to").default_value(std::string{"localhost:8080"});

        program.add_argument("--on-exit")
            .choices(
                magic_enum::enum_name(OnExitBehavior::WAIT_FOR_QUERY_TERMINATION),
                magic_enum::enum_name(OnExitBehavior::STOP_QUERIES),
                magic_enum::enum_name(OnExitBehavior::DO_NOTHING))
            .default_value(std::string(magic_enum::enum_name(OnExitBehavior::DO_NOTHING)))
            .help(fmt::format(
                "on exit behavior: [{}]",
                fmt::join(
                    std::views::transform(
                        magic_enum::enum_values<OnExitBehavior>(),
                        [](const auto& exitBehavior) { return magic_enum::enum_name(exitBehavior); }),
                    ", ")));

        program.add_argument("-e", "--error-behaviour")
            .choices("FAIL_FAST", "RECOVER", "CONTINUE_AND_FAIL")
            .help(
                "Fail and return non-zero exit code on first error, ignore error and continue, or continue and return non-zero exit code");
        program.add_argument("-f").default_value("TEXT").choices("TEXT", "JSON").help("Output format");
        /// query optimizer config
        program.add_argument("--optimizer")
            .default_value<std::vector<std::string>>({})
            .append()
            .help("changes optimizer default values. e.g. join_strategy=HASH_JOIN");

        /// companion statistic config
        program.add_argument("--companion-statistic").flag().help("Deploy a companion statistic query alongside every SELECT query");
        program.add_argument("--companion-field")
            .default_value(std::string{"price"})
            .help("Field name for the companion statistic (default: price)");
        program.add_argument("--companion-field-2")
            .help("Field name for the SECOND companion statistic (when paired with --companion-condition-2). "
                  "If omitted, the secondary statistic reuses --companion-field. Use a different field here to "
                  "deploy a second build branch monitoring a separate column — the data source defers emission "
                  "until both build branches have spliced in, and the source still serves a single thread to "
                  "all of them.");
        program.add_argument("--companion-metric")
            .default_value(std::string(magic_enum::enum_name(NES::Metric::Cardinality)))
            .choices(
                magic_enum::enum_name(NES::Metric::Cardinality),
                magic_enum::enum_name(NES::Metric::MinVal),
                magic_enum::enum_name(NES::Metric::MaxVal),
                magic_enum::enum_name(NES::Metric::Rate),
                magic_enum::enum_name(NES::Metric::Average),
                magic_enum::enum_name(NES::Metric::Selectivity))
            .help("Metric type for the companion statistic (default: Cardinality)");
        program.add_argument("--companion-window-size-ms")
            .default_value(std::string{"1000000"})
            .help("Window size in milliseconds for the companion statistic (default: 1000000)");
        program.add_argument("--companion-window-advance-ms").help("Window advance in milliseconds; if omitted, uses a tumbling window");
        program.add_argument("--companion-event-time-field").help("Event-time field name; if omitted, uses ingestion time");
        program.add_argument("--companion-condition")
            .help("SQL filter expression interpreted as a predicate over EquiWidthHistogram bin fields "
                  "(BINSTART, BINEND, BINCOUNTER) when paired with --companion-metric MinVal. Gates the probe: "
                  "only bins matching the predicate flow to the swap callback. The callback then drives the "
                  "workload switch to --companion-target-value.");
        program.add_argument("--companion-target-value")
            .default_value(std::string{"1"})
            .help("Target value for the workload-switch gate when --companion-condition's predicate matches. "
                  "The callback is idempotent: if the gate is already at this value, the firing is a quiet no-op. "
                  "Default: 1.");
        program.add_argument("--companion-condition-2")
            .help("Second gated-probe predicate. Deploys an additional probe pipeline reading the same build "
                  "branch's histogram, with this Selection predicate. When it fires, the callback sets the gate "
                  "to --companion-target-value-2. Lets a constant-workload run flip ONCE to the optimal regime "
                  "and an A↔B alternating workload flip back and forth as the histogram shifts.");
        program.add_argument("--companion-target-value-2")
            .default_value(std::string{"0"})
            .help("Target value for the workload-switch gate when --companion-condition-2's predicate matches. Default: 0.");
        program.add_argument("--companion-histogram-min")
            .default_value(std::string{"0"})
            .help("Minimum value for the EquiWidthHistogram bucket range (only used with --companion-metric MinVal). "
                  "Values below this fall outside the histogram. Default: 0.");
        program.add_argument("--companion-histogram-max")
            .default_value(std::string{"1000"})
            .help("Maximum value for the EquiWidthHistogram bucket range (only used with --companion-metric MinVal). "
                  "Values above this fall outside the histogram. Default: 1000.");
        program.add_argument("--companion-host")
            .default_value(std::string{"localhost:8080"})
            .help("Worker host for the companion statistic sink (default: localhost:8080)");
        program.add_argument("--companion-switch-to-sql")
            .help("Full SELECT SQL to deploy when the companion statistic first fires, replacing the original query");
        program.add_argument("--companion-switch-name")
            .default_value(std::string{"filter_order"})
            .help("Name of the workload-switch gate (SwitchRegistry slot) used to flip between two filter-chain "
                  "pipelines without redeploying. The data sink is gated with expected=0, the paired sink "
                  "(--companion-switch-to-sql) with expected=1. Default: filter_order");
        program.add_argument("--baseline-prometheus")
            .flag()
            .help("Run the Prometheus SOTA baseline instead of the native in-engine statistic path. The companion "
                  "build branch routes the monitored field into a PrometheusSink (which builds the histogram and "
                  "exposes it for scraping) instead of the StatisticBuild→StoreWriter→Probe→GrpcSink chain. Switching "
                  "is driven by the coordinator polling Prometheus rather than by gRPC probe reports.");
        program.add_argument("--prometheus-server-url")
            .default_value(std::string{"0.0.0.0:9464"})
            .help("host:port the Prometheus-baseline sink's exposer binds its /metrics endpoint to (scraped by the "
                  "external Prometheus instance). Only used with --baseline-prometheus. Default: 0.0.0.0:9464");
        program.add_argument("--baseline-prometheus-query-url")
            .default_value(std::string{"localhost:9595"})
            .help("host:port of the Prometheus server's query API that the coordinator poll loop GETs "
                  "(/api/v1/query). Only used with --baseline-prometheus. Default: localhost:9595");
        program.add_argument("--baseline-promql")
            .default_value(std::string{""})
            .help("PromQL instant-query expression returning a scalar that the poll loop thresholds to pick the "
                  "filter order. Empty (default) auto-builds histogram_quantile(0.5, rate(<FIELD>_bucket[30s])) from "
                  "--companion-field.");
        program.add_argument("--baseline-switch-threshold")
            .default_value(std::string{"888.49"})
            .help("Poll-loop decision threshold on the PromQL value: >= threshold selects the price-first order "
                  "(switch=1), below selects bid-first (switch=0). Default 888.49 (between regime A's ~500 and "
                  "regime B's ~1277 median price).");
        program.add_argument("--baseline-poll-interval-ms")
            .default_value(std::string{"1000"})
            .help("How often (ms) the coordinator poll loop queries Prometheus. Default: 1000");

#ifdef EMBED_ENGINE
        /// single node worker config
        program.add_argument("--")
            .help("arguments passed to the worker config, e.g., `-- --worker.query_engine.number_of_worker_threads=10`")
            .default_value(std::vector<std::string>{})
            .remaining();
#endif

        try
        {
            program.parse_args(argc, argv);
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << "\n";
            std::cerr << program;
            return 1;
        }

        if (program.get<bool>("-d"))
        {
            NES::Logger::getInstance()->changeLogLevel(NES::LogLevel::LOG_DEBUG);
        }

        const auto defaultOutputFormatOpt = magic_enum::enum_cast<NES::StatementOutputFormat>(program.get<std::string>("-f"));
        if (not defaultOutputFormatOpt.has_value())
        {
            NES_ERROR("Invalid output format: {}", program.get<std::string>("-f"));
            return 1;
        }
        const auto defaultOutputFormat = defaultOutputFormatOpt.value();


        const NES::ErrorBehaviour errorBehaviour = [&]
        {
            if (program.is_used("-e"))
            {
                return magic_enum::enum_cast<NES::ErrorBehaviour>(program.get<std::string>("-e")).value();
            }
            if (interactiveMode)
            {
                return NES::ErrorBehaviour::RECOVER;
            }
            return NES::ErrorBehaviour::FAIL_FAST;
        }();

        NES::QueryOptimizerConfiguration queryOptimizerConfig;

        if (program.is_used("--optimizer"))
        {
            auto optimizerConfigVec = program.get<std::vector<std::string>>("--optimizer");
            std::unordered_map<std::string, std::string> optimizerRawConfig;

            for (const auto& optimizerConfigString : optimizerConfigVec)
            {
                if (auto pos = optimizerConfigString.find("="); pos != std::string::npos)
                {
                    const std::string identifier = optimizerConfigString.substr(0, pos);
                    const std::string value = optimizerConfigString.substr(pos + 1);
                    optimizerRawConfig[identifier] = value;
                }
                else
                {
                    NES_ERROR("Invalid optimizer argument. Requires argument like 'CONFIG=VALUE' but got '{}'", optimizerConfigString)
                    return 1;
                }
            }
            queryOptimizerConfig.overwriteConfigWithCommandLineInput(optimizerRawConfig);
        }


        auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
        auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
        auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
        std::shared_ptr<NES::QueryManager> queryManager{};
        auto binder = NES::StatementBinder{
            sourceCatalog, [](auto&& pH1) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(pH1)>(pH1)); }};

#ifdef EMBED_ENGINE
        enable_memcom();
        auto confVec = program.get<std::vector<std::string>>("--");

        const int singleNodeArgC = static_cast<int>(confVec.size() + 1);
        std::vector<const char*> singleNodeArgV;
        singleNodeArgV.reserve(singleNodeArgC + 1);
        singleNodeArgV.push_back("nes-single-node-worker"); /// dummy option as arg expects first arg to be the program name
        for (auto& arg : confVec)
        {
            singleNodeArgV.push_back(arg.c_str());
        }
        auto singleNodeWorkerConfig = NES::loadConfiguration<NES::SingleNodeWorkerConfiguration>(singleNodeArgC, singleNodeArgV.data())
                                          .value_or(NES::SingleNodeWorkerConfiguration{});

        /// Derive a routable Host from the gRPC bind address.
        /// The default bind address [::]:8080 is a wildcard, so we use localhost:<port> instead.
        const auto grpcBind = singleNodeWorkerConfig.grpcAddressUri.getValue();
        const auto grpcAddr = "localhost" + grpcBind.substr(grpcBind.rfind(':'));
        const auto dataAddr = singleNodeWorkerConfig.dataAddress.getValue();
        const NES::WorkerConfig workerConfig{
            .host = NES::Host(grpcAddr),
            .dataAddress = dataAddr,
            .maxOperators = NES::Capacity(NES::CapacityKind::Unlimited{}),
            .downstream = {},
            .config = singleNodeWorkerConfig,
        };
        workerCatalog->addWorker(workerConfig.host, workerConfig.dataAddress, workerConfig.maxOperators, workerConfig.downstream);
        queryManager = std::make_shared<NES::QueryManager>(workerCatalog, NES::createEmbeddedBackend(singleNodeWorkerConfig));
        NES::SourceStatementHandler sourceStatementHandler{sourceCatalog, NES::DefaultHost(grpcAddr)};
        NES::SinkStatementHandler sinkStatementHandler{sinkCatalog, NES::DefaultHost(grpcAddr)};
#else
        queryManager = std::make_shared<NES::QueryManager>(workerCatalog, NES::createGRPCBackend());
        NES::SourceStatementHandler sourceStatementHandler{sourceCatalog, NES::RequireHostConfig{}};
        NES::SinkStatementHandler sinkStatementHandler{sinkCatalog, NES::RequireHostConfig{}};
#endif
        NES::TopologyStatementHandler topologyStatementHandler{queryManager, workerCatalog};
        auto queryOptimizer = std::make_shared<NES::QueryOptimizer>(queryOptimizerConfig, sourceCatalog, sinkCatalog, workerCatalog);
        auto queryStatementHandler = std::make_shared<NES::QueryStatementHandler>(queryManager, queryOptimizer);
        auto submitQueryFn = [queryManager, queryOptimizer](NES::LogicalPlan plan) -> std::expected<NES::QueryId, NES::Exception>
        {
            std::stringstream beforeSs;
            beforeSs << plan;
            fprintf(stderr, "DEBUG: Statistic query BEFORE optimization:\n%s\n", beforeSs.str().c_str());
            auto distributedPlan = queryOptimizer->optimize(plan);
            std::stringstream afterSs;
            afterSs << distributedPlan.getGlobalPlan();
            fprintf(stderr, "DEBUG: Statistic query AFTER optimization:\n%s\n", afterSs.str().c_str());
            auto registerResult = queryManager->registerQuery(distributedPlan);
            if (!registerResult.has_value())
            {
                return std::unexpected(registerResult.error());
            }
            auto distributedQueryId = registerResult.value();
            auto startResult = queryManager->start(distributedQueryId);
            if (!startResult.has_value())
            {
                return std::unexpected(NES::QueryStartFailed("Could not start statistic query: {}", startResult.error().front()));
            }
            return NES::QueryId::createDistributed(distributedQueryId);
        };
        NES::StatisticRequestHandler statisticRequestHandler{
            NES::StatisticCoordinator{std::make_unique<NES::DefaultStatisticQueryGenerator>(), submitQueryFn}};
        auto coordinatorAddr = statisticRequestHandler.startGrpcServer();
        NES_INFO("StatisticCoordinator gRPC server listening on {}", coordinatorAddr);

        auto parseConditionExpression = [](const std::string& conditionStr) -> std::optional<NES::LogicalFunction>
        {
            /// Wrap the raw SQL expression in a synthetic SELECT so the existing parser can handle it.
            /// The dummy source and sink names are irrelevant — the parser only needs the syntactic
            /// shape of a complete query; we then walk the AST and lift just the Selection's
            /// predicate. The INTO clause is required (`Query does not contain sink` otherwise).
            /// Catch all exceptions: a malformed expression must not crash the REPL silently. Print
            /// a clear diagnostic to stderr and return nullopt so the caller skips wiring this
            /// predicate into the companion request.
            const auto sql = fmt::format("SELECT * FROM _nes_stat_dummy_ WHERE {} INTO _nes_stat_dummy_sink_", conditionStr);
            try
            {
                auto plan = NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql);
                auto selections = NES::getOperatorByType<NES::SelectionLogicalOperator>(plan);
                if (selections.empty())
                {
                    std::cerr << "[--companion-condition] No SelectionLogicalOperator found in parsed expression: '" << conditionStr
                              << "' — ignoring.\n";
                    return std::nullopt;
                }
                return selections.front()->getPredicate();
            }
            catch (const std::exception& e)
            {
                std::cerr << "[--companion-condition] Failed to parse expression: '" << conditionStr << "': " << e.what()
                          << " — ignoring.\n";
                return std::nullopt;
            }
            catch (...)
            {
                std::cerr << "[--companion-condition] Unknown exception parsing expression: '" << conditionStr << "' — ignoring.\n";
                return std::nullopt;
            }
        };

        /// A second binder instance sharing the same source catalog. Used inside the companion callback,
        /// which runs on a gRPC thread and cannot use the binder that was moved into Repl.
        /// Wrapped in shared_ptr so the non-copyable StatementBinder can be captured by a std::function.
        auto callbackBinder = std::make_shared<NES::StatementBinder>(
            sourceCatalog, [](auto&& pH1) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(pH1)>(pH1)); });

        /// Each entry is one (predicate, callback) pair to register with collectWorkloadStatistic.
        /// The first call deploys the data query + build branch + first gated probe; subsequent
        /// calls hit the "registry already has this key" branch in collectWorkloadStatistic which
        /// deploys ONLY an additional gated probe + callback.
        std::vector<NES::RequestStatisticBuildStatement> companionStatisticRequests;
        /// Prometheus-baseline poll loop thread. Declared in this outer scope so it outlives the
        /// companion-setup block where it is started and remains joinable across replClient.run();
        /// it is stop-requested after run() returns. std::jthread joins on destruction.
        std::jthread baselinePollThread;
        std::optional<std::function<void(NES::DistributedQueryId, const std::string&, NES::Statistic::StatisticId)>>
            onCompanionAssociatedWithQuery = std::nullopt;
        if (program.get<bool>("--companion-statistic"))
        {
            const auto metric = magic_enum::enum_cast<NES::Metric>(program.get<std::string>("--companion-metric")).value();

            std::optional<uint64_t> windowAdvanceMs;
            if (program.is_used("--companion-window-advance-ms"))
                windowAdvanceMs = std::stoull(program.get<std::string>("--companion-window-advance-ms"));

            std::optional<std::string> eventTimeFieldName;
            if (program.is_used("--companion-event-time-field"))
                eventTimeFieldName = program.get<std::string>("--companion-event-time-field");

            std::optional<NES::LogicalFunction> condition;
            if (program.is_used("--companion-condition"))
                condition = parseConditionExpression(program.get<std::string>("--companion-condition"));
            std::optional<NES::LogicalFunction> condition2;
            if (program.is_used("--companion-condition-2"))
                condition2 = parseConditionExpression(program.get<std::string>("--companion-condition-2"));
            const auto targetSwitchValue1 = std::stoll(program.get<std::string>("--companion-target-value"));
            const auto targetSwitchValue2 = std::stoll(program.get<std::string>("--companion-target-value-2"));

            std::string switchToSql;
            if (program.is_used("--companion-switch-to-sql"))
                switchToSql = program.get<std::string>("--companion-switch-to-sql");

            struct AdaptiveSwapState
            {
                std::mutex mutex;
                std::optional<NES::DistributedQueryId> currentQueryId;
                std::string currentSql; /// SQL of the currently running query
                std::string nextSql; /// SQL to deploy on the next trigger
                /// Set by onCompanionAssociatedWithQuery after the initial workload-companion deploy.
                /// The swap callback reuses this id so a single registry entry remains valid across
                /// all re-deployments — the build branch on every spliced plan reports under the same
                /// statisticId and therefore matches the same trigger entry.
                std::optional<NES::Statistic::StatisticId> statisticId;
                /// Workload-switch mode: current gate value. The callback flips this between 0 and 1
                /// on each fire instead of redeploying. Updated under `mutex`.
                int64_t currentSwitchValue = 0;
            };

            auto swapState = std::make_shared<AdaptiveSwapState>();
            swapState->nextSql = switchToSql;

            /// Workload-switch mode is active when paired SQL is provided. In that case the swap
            /// callback flips the named gate via gRPC SetSwitch instead of stopping and redeploying
            /// the query.
            const auto switchName = program.get<std::string>("--companion-switch-name");
            const auto workerServerUri = program.get<std::string>("-s");
            const bool workloadSwitchMode = not switchToSql.empty();

            /// Prometheus-baseline mode: route each companion build branch into a PrometheusSink
            /// (see DefaultStatisticQueryGenerator::generateWorkloadBranchPrometheus). Carried to
            /// collectWorkloadStatistic via the per-request "prometheus_server_url" option; an empty
            /// value selects the native in-engine path. The gated SetSwitch callback below stays
            /// installed but is inert in this mode (the PrometheusSink emits no gRPC reports, so it
            /// never fires) — switching is driven by the coordinator's Prometheus poll loop.
            const bool baselinePrometheus = program.get<bool>("--baseline-prometheus");
            const auto prometheusServerUrl = program.get<std::string>("--prometheus-server-url");

            /// Shared filter-order switch action for both paths: the native gated callback (below)
            /// and the Prometheus poll loop. Owns the gate-state guard so a SetSwitch is issued only
            /// on a real regime change.
            auto switchClient = std::make_shared<WorkloadSwitchClient>(workerServerUri);

            /// WorkloadDomain.queryId / operatorId are placeholders here — collectWorkloadStatistic
            /// resolves the actual splice target from the data query's LogicalPlan at deploy time.
            const NES::CollectionDomain collectionDomain = NES::WorkloadDomain{
                .queryId = NES::QueryId::invalid(),
                .operatorId = NES::INVALID_OPERATOR_ID,
                .fieldName = program.get<std::string>("--companion-field")};

            /// Splice-related captures for the swap callback re-deploy path. Built once here so the
            /// swap callback can re-generate the build branch on every fire without having to look
            /// up either the original request or the coordinator's generator.
            const auto windowSizeMs = std::stoull(program.get<std::string>("--companion-window-size-ms"));
            const auto companionField = program.get<std::string>("--companion-field");
            const auto companionHost = program.get<std::string>("--companion-host");
            const auto swapCoordinatorAddr = coordinatorAddr;
            auto swapGenerator = std::make_shared<NES::DefaultStatisticQueryGenerator>();

            /// Prometheus-baseline: spawn the coordinator-side poll loop. It periodically queries the
            /// Prometheus server (PromQL histogram_quantile over the sink's scraped buckets) to detect
            /// the workload regime and flips the filter-order gate through the SAME `switchClient` the
            /// native callback uses — so both adaptive paths share the act and differ only in detection
            /// (in-engine gated probe vs. Prometheus poll). It is safe to start before the query
            /// deploys: queryPrometheusScalar returns nullopt until Prometheus has data, so early ticks
            /// are harmless no-ops. Requires workloadSwitchMode (a switchable alternate to flip).
            if (baselinePrometheus && workloadSwitchMode)
            {
                const auto pollQueryUrl = program.get<std::string>("--baseline-prometheus-query-url");
                const auto pollIntervalMs = static_cast<int>(std::stoull(program.get<std::string>("--baseline-poll-interval-ms")));
                const double switchThreshold = std::stod(program.get<std::string>("--baseline-switch-threshold"));
                std::string promql = program.get<std::string>("--baseline-promql");
                if (promql.empty())
                {
                    /// Default: median of the monitored field over the last 30s, from the sink's
                    /// histogram buckets. Metric name = uppercased field (the PrometheusSink names the
                    /// metric after the projected field), e.g. price -> PRICE_bucket.
                    std::string fieldUpper = companionField;
                    for (auto& ch : fieldUpper)
                    {
                        if (ch >= 'a' && ch <= 'z')
                        {
                            ch = static_cast<char>(ch - 'a' + 'A');
                        }
                    }
                    promql = "histogram_quantile(0.5, rate(" + fieldUpper + "_bucket[4s]))";
                }
                NES_INFO(
                    "Prometheus-baseline poll loop: query={} every {}ms, PromQL='{}', threshold={} "
                    "(>=threshold -> gate '{}'=1 price-first, else 0 bid-first)",
                    pollQueryUrl,
                    pollIntervalMs,
                    promql,
                    switchThreshold,
                    switchName);
                baselinePollThread = std::jthread(
                    [switchClient, switchName, pollQueryUrl, promql, switchThreshold, pollIntervalMs](std::stop_token stopToken)
                    {
                        while (not stopToken.stop_requested())
                        {
                            /// Interruptible sleep in small slices so shutdown is prompt.
                            for (int slept = 0; slept < pollIntervalMs && not stopToken.stop_requested(); slept += 50)
                            {
                                std::this_thread::sleep_for(std::chrono::milliseconds{50});
                            }
                            if (stopToken.stop_requested())
                            {
                                break;
                            }
                            const auto value = NES::repl_baseline::queryPrometheusScalar(pollQueryUrl, promql);
                            if (not value.has_value())
                            {
                                continue; /// Prometheus unreachable or no samples in the rate() window yet.
                            }
                            /// Regime decision: median >= threshold ⇒ regime B (price-first, gate 1);
                            /// below ⇒ regime A (bid-first, gate 0). setSwitch's guard makes this a
                            /// no-op unless the regime actually changed.
                            switchClient->setSwitch(switchName, *value >= switchThreshold ? 1 : 0);
                        }
                    });
            }
            /// Factory: builds one RequestStatisticBuildStatement bound to a specific predicate
            /// + target switch value. The callback closure captures `targetSwitchValue` so each
            /// generated request drives the workload-switch gate to its own regime when it fires.
            /// All other captures are shared across the requests (build chain spec, swapState,
            /// re-deploy plumbing, gRPC stub config).
            auto makeRequest = [&](std::optional<NES::LogicalFunction> cond,
                                   int64_t targetSwitchValue,
                                   std::optional<std::string> fieldOverride = std::nullopt)
            {
                /// If fieldOverride is set, construct a fresh WorkloadDomain with that field
                /// name — different fieldName → different registry key → distinct build branch.
                NES::CollectionDomain perRequestDomain = collectionDomain;
                if (fieldOverride.has_value())
                {
                    perRequestDomain = NES::WorkloadDomain{
                        .queryId = NES::QueryId::invalid(), .operatorId = NES::INVALID_OPERATOR_ID, .fieldName = *fieldOverride};
                }
                return NES::RequestStatisticBuildStatement{
                .domain = perRequestDomain,
                .metric = metric,
                .windowSizeMs = windowSizeMs,
                .windowAdvanceMs = windowAdvanceMs,
                .eventTimeFieldName = eventTimeFieldName,
                .conditionTrigger = NES::ConditionTrigger{
                    .condition = cond,
                    .callback =
                        [swapState,
                         queryStatementHandler,
                         callbackBinder,
                         switchToSql,
                         metric,
                         windowSizeMs,
                         windowAdvanceMs,
                         eventTimeFieldName,
                         cond,
                         companionField,
                         companionHost,
                         swapCoordinatorAddr,
                         swapGenerator,
                         workloadSwitchMode,
                         switchName,
                         switchClient,
                         targetSwitchValue](
                            NES::Statistic::StatisticId statId,
                            NES::Windowing::TimeMeasure startTs,
                            NES::Windowing::TimeMeasure endTs)
                    {
                            /// Workload-switch path: set the named gate to the regime favored by THIS
                            /// trigger via gRPC. No query stop/redeploy — the source thread keeps
                            /// running, and the merged plan's two chains see the new gate value on
                            /// their next buffer.
                            ///
                            /// Idempotency: each gated probe represents a single workload regime; its
                            /// firing condition holds only while that regime is favored. The intended
                            /// target switch value for this trigger is `targetSwitchValue` (set to 1
                            /// — the alternate filter chain — by default). If the gate is already at
                            /// the target, skip the gRPC call. Without this guard the previous code
                            /// blindly toggled 0↔1 on every fire, producing one redeploy per matching
                            /// histogram bin per probe tick instead of one redeploy per regime change.
                            if (workloadSwitchMode)
                            {
                                /// Flip the named filter-order gate through the shared switch client
                                /// (gRPC SetSwitch + the once-per-regime-change guard). This is the exact
                                /// same action the Prometheus-baseline poll loop performs, so the two
                                /// adaptive paths differ only in HOW the regime is detected (in-engine
                                /// gated probe vs. Prometheus poll), not in how the switch is applied.
                                switchClient->setSwitch(switchName, targetSwitchValue);
                                return;
                            }

                            std::optional<NES::DistributedQueryId> currentQueryId;
                            std::string currentSql;
                            std::string nextSql;
                            {
                                std::lock_guard lock(swapState->mutex);
                                if (!swapState->currentQueryId.has_value() || swapState->nextSql.empty())
                                {
                                    return;
                                }
                                currentQueryId = swapState->currentQueryId;
                                currentSql = swapState->currentSql;
                                nextSql = swapState->nextSql;
                            }

                            auto stopResult = (*queryStatementHandler)(NES::DropQueryStatement{.id = *currentQueryId});
                            if (!stopResult.has_value())
                            {
                                return;
                            }

                            auto bindResult = callbackBinder->parseAndBindSingle(nextSql);
                            if (!bindResult.has_value())
                            {
                                return;
                            }
                            auto* queryStmt = std::get_if<NES::QueryStatement>(&bindResult.value());
                            if (!queryStmt)
                            {
                                return;
                            }

                            /// Re-splice the workload build branch into the new query's plan so the
                            /// merged plan keeps reporting under the original statisticId — the existing
                            /// registry entry continues firing this very callback on each window close.
                            std::optional<NES::Statistic::StatisticId> originalStatisticId;
                            {
                                std::lock_guard lock(swapState->mutex);
                                originalStatisticId = swapState->statisticId;
                            }
                            if (originalStatisticId.has_value())
                            {
                                try
                                {
                                    const auto sources = NES::getOperatorByType<NES::SourceNameLogicalOperator>(queryStmt->plan);
                                    if (sources.size() == 1)
                                    {
                                        const NES::WorkloadDomain workloadDomain{
                                            .queryId = NES::QueryId::invalid(),
                                            .operatorId = NES::INVALID_OPERATOR_ID,
                                            .fieldName = companionField};
                                        NES::RequestStatisticBuildStatement swapRequest{
                                            .domain = workloadDomain,
                                            .metric = metric,
                                            .windowSizeMs = windowSizeMs,
                                            .windowAdvanceMs = windowAdvanceMs,
                                            .eventTimeFieldName = eventTimeFieldName,
                                            .conditionTrigger = NES::ConditionTrigger{.condition = cond, .callback = {}},
                                            .options = {{"host", companionHost}}};
                                        const NES::LogicalOperator spliceLeaf{sources.front()};
                                        auto branch = swapGenerator->generateWorkloadBranch(
                                            workloadDomain, swapRequest, *originalStatisticId, swapCoordinatorAddr, spliceLeaf);
                                        queryStmt->plan = NES::addRootOperators(queryStmt->plan, branch.getRootOperators());
                                    }
                                }
                                catch (const std::exception&)
                                {
                                }
                            }

                            auto startResult = (*queryStatementHandler)(*queryStmt);
                            if (!startResult.has_value())
                            {
                                return;
                            }

                            {
                                std::lock_guard lock(swapState->mutex);
                                swapState->currentQueryId = startResult->id;
                                swapState->currentSql = nextSql;
                                swapState->nextSql = currentSql;
                            }
                    }},
                .options
                = {{"host", program.get<std::string>("--companion-host")},
                   /// paired_sql + switch_name carry the workload-switch alternate plan through to
                   /// Repl::Impl::executeQuery. Empty paired_sql means: no alternate, use the normal
                   /// collectWorkloadStatistic flow.
                   {"paired_sql", switchToSql},
                   {"switch_name", program.get<std::string>("--companion-switch-name")},
                   /// min/max are EquiWidthHistogram bucket-range bounds; read by
                   /// DefaultStatisticQueryGenerator::createAggregationFunction. In Prometheus-baseline
                   /// mode they also seed the PrometheusSink's equi-width histogram_min/max_value.
                   {"min", program.get<std::string>("--companion-histogram-min")},
                   {"max", program.get<std::string>("--companion-histogram-max")},
                   /// Non-empty only with --baseline-prometheus: selects the PrometheusSink build
                   /// branch in collectWorkloadStatistic and binds the sink's exposer to this address.
                   {"prometheus_server_url", baselinePrometheus ? prometheusServerUrl : std::string{}}}};
            };

            /// Build the primary statement. When the first --companion-condition predicate fires,
            /// the gate moves to --companion-target-value (default 1).
            companionStatisticRequests.push_back(makeRequest(condition, targetSwitchValue1));
            /// Optional secondary statement: a second gated probe with its own predicate +
            /// target value. With Phase 2 (multi-splice), the data source defers emission
            /// until ALL build branches splice in — count carried via the "expected_splice_count"
            /// option set just below.
            if (condition2.has_value())
            {
                std::optional<std::string> field2;
                if (program.is_used("--companion-field-2"))
                {
                    field2 = program.get<std::string>("--companion-field-2");
                }
                companionStatisticRequests.push_back(makeRequest(condition2, targetSwitchValue2, field2));
            }
            /// Set expected_splice_count on EVERY request so collectWorkloadStatistic (called
            /// once per request) reads the same total when stamping the data plan's source.
            /// Only the FIRST call uses this to set up the deferred-start budget; later calls
            /// hit the "registry already exists" path and don't re-stamp.
            for (auto& req : companionStatisticRequests)
            {
                req.options["expected_splice_count"] = std::to_string(companionStatisticRequests.size());
            }
            onCompanionAssociatedWithQuery
                = [swapState](NES::DistributedQueryId id, const std::string& sql, NES::Statistic::StatisticId statId)
            {
                std::lock_guard lock(swapState->mutex);
                swapState->currentQueryId = std::move(id);
                swapState->currentSql = sql;
                if (not swapState->statisticId.has_value())
                {
                    swapState->statisticId = statId;
                }
            };
        }

        NES::Repl replClient{
            std::move(sourceStatementHandler),
            std::move(sinkStatementHandler),
            std::move(topologyStatementHandler),
            queryStatementHandler,
            std::move(statisticRequestHandler),
            std::move(binder),
            errorBehaviour,
            defaultOutputFormat,
            interactiveMode,
            SignalHandler::terminationToken(),
            std::move(companionStatisticRequests),
            std::move(onCompanionAssociatedWithQuery)};
        replClient.run();

        /// Stop the Prometheus-baseline poll loop (no-op if it was never started). std::jthread also
        /// requests stop + joins on destruction, but doing it here ends the loop promptly once the
        /// REPL exits, before the rest of teardown.
        baselinePollThread.request_stop();

        bool hasError = false;
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) validated by argparse .choices()
        switch (magic_enum::enum_cast<OnExitBehavior>(program.get<std::string>("--on-exit")).value())
        {
            case OnExitBehavior::STOP_QUERIES:
                for (auto& query : queryManager->getRunningQueries())
                {
                    auto result = queryStatementHandler->operator()(NES::DropQueryStatement{.id = query});
                    const NES::StatementOutputAssembler<NES::DropQueryStatementResult> assembler{};
                    if (!result.has_value())
                    {
                        NES_ERROR("Could not stop query: {}", result.error().what());
                        hasError = true;
                        continue;
                    }
                    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) validated by argparse .choices()
                    printStatementResult(
                        std::cout, magic_enum::enum_cast<NES::StatementOutputFormat>(program.get("-f")).value(), result.value());
                }
                [[clang::fallthrough]];
            case OnExitBehavior::WAIT_FOR_QUERY_TERMINATION:
                while (!queryManager->getRunningQueries().empty())
                {
                    NES_DEBUG("Waiting for termination")
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                break;
            case OnExitBehavior::DO_NOTHING:
                break;
        }

        if (hasError)
        {
            return 1;
        }
        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
