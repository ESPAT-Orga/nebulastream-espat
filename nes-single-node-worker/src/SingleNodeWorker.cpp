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

#include <SingleNodeWorker.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Configurations/ConfigValuePrinter.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Listeners/QueryLog.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Pipelines/SwitchableCompiledExecutablePipelineStage.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <BackpressureStatisticStdoutEmitter.hpp>
#include <CompiledQueryPlan.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <LatencyListener.hpp>
#include <NetworkOptions.hpp>
#include <QueryCompiler.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SwitchRegistry.hpp>
#include <ThroughputListener.hpp>
#include <WorkerStatus.hpp>

extern void initNetworkServices(const std::string& connectionAddr, const NES::Host& host, const NES::NetworkOptions& options);

namespace NES
{

struct SingleNodeWorker::PendingPlan
{
    std::unique_ptr<CompiledQueryPlan> qep;
    LogicalPlan dataLogicalPlan; /// kept so attachAlternatePipeline can re-resolve compilation if needed
};

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration, const Host& host)
    : listener(std::make_shared<CompositeStatisticListener>())
    , configuration(configuration)
    , pendingPlans(std::make_unique<folly::Synchronized<std::unordered_map<QueryId, std::shared_ptr<PendingPlan>>>>())
{
    {
        std::stringstream configStr;
        ConfigValuePrinter printer(configStr);
        SingleNodeWorkerConfiguration(configuration).accept(printer);
        NES_INFO("Starting SingleNodeWorker {} with configuration:\n{}", host.getRawValue(), configStr.str());
    }
    if (configuration.enableGoogleEventTrace.getValue())
    {
        auto googleTracePrinter = std::make_shared<GoogleEventTracePrinter>(
            fmt::format("trace_{}_{:%Y-%m-%d_%H-%M-%S}_{:d}.json", host.getRawValue(), std::chrono::system_clock::now(), ::getpid()));
        googleTracePrinter->start();
        listener->addListener(googleTracePrinter);
    }

    /// Writing the current throughput to the log
    auto throughputCallback = [](const ThroughputListener::CallBackParams& callBackParams)
    {
        /// Helper function to format throughput in SI units
        auto formatThroughput = [](double throughput, const std::string_view suffix)
        {
            constexpr std::array UNITS_THROUGHPUT = {std::to_array<const char*>({"", "k", "M", "G", "T"})};
            uint64_t unitIndex = 0;

            while (throughput >= 1000 && unitIndex < UNITS_THROUGHPUT.size() - 1)
            {
                throughput /= 1000;
                ++unitIndex;
            }

            return fmt::format("{:.3f} {}{}/s", throughput, UNITS_THROUGHPUT[unitIndex], suffix);
        };

        const auto tuplesPerSecondMessage = formatThroughput(callBackParams.throughputInTuplesPerSec, "Tup");
        std::cout << fmt::format(
            "Throughput for queryId {} in window {}-{} is {}\n",
            callBackParams.queryId,
            callBackParams.windowStart,
            callBackParams.windowEnd,
            tuplesPerSecondMessage)
                  << std::flush;
    };
    const auto timeIntervalInMilliSeconds = configuration.workerConfiguration.throughputListenerInterval.getValue();
    const auto throughputListener = std::make_shared<ThroughputListener>(timeIntervalInMilliSeconds, throughputCallback);
    listener->addQueryEngineListener(throughputListener);

    if (configuration.workerConfiguration.latencyListener.getValue())
    {
        auto latencyCallBack = [](const LatencyListener::CallBackParams& callBackParams)
        {
            /// Helper function to format latency in SI units
            auto formatLatency = [](const std::chrono::duration<double> latency)
            {
                constexpr std::array UNITS_LATENCY = {std::to_array<const char*>({"", "m", "u", "n"})};
                auto latencyCount = latency.count();
                uint64_t unitIndex = 0;

                while (latencyCount <= 1 && unitIndex < UNITS_LATENCY.size() - 1)
                {
                    latencyCount *= 1000;
                    ++unitIndex;
                }

                return fmt::format("{:.3f} {}s", latencyCount, UNITS_LATENCY[unitIndex]);
            };

            const auto latencyMessage = formatLatency(callBackParams.averageLatency);
            std::cout << fmt::format(
                "Latency for queryId {} and {} tasks over duration {}-{} is {}\n",
                callBackParams.queryId,
                callBackParams.numberOfTasks,
                callBackParams.firstTaskTimestamp,
                callBackParams.lastTaskTimestamp,
                latencyMessage);
        };

        constexpr auto numberOfTasks = 1;
        const auto latencyListener = std::make_shared<LatencyListener>(latencyCallBack, numberOfTasks);
        listener->addQueryEngineListener(latencyListener);
    }


    /// Stdout emitter ports the BackpressureStatisticListener mechanism from the adaptive-network-sinks branch.
    /// Disabled by default; only the network-sink benchmark consumes the per-event stdout lines.
    std::shared_ptr<BackpressureStatisticListener> backpressureStatisticListener;
    if (configuration.workerConfiguration.backpressureStatisticListener.getValue())
    {
        backpressureStatisticListener = std::make_shared<BackpressureStatisticStdoutEmitter>();
    }

    nodeEngine = NodeEngineBuilder(
                     configuration.workerConfiguration,
                     copyPtr(listener),
                     configuration.networkSinkSendingStrategy.getValue(),
                     backpressureStatisticListener)
                     .build(host);
    compiler = std::make_unique<QueryCompilation::QueryCompiler>(
        configuration.workerConfiguration.defaultQueryExecution, nodeEngine->getStatisticStore());

    if (!configuration.dataAddress.getValue().empty())
    {
        const auto& networkConfig = configuration.workerConfiguration.network;
        initNetworkServices(
            configuration.dataAddress.getValue(),
            host,
            NetworkOptions{
                .senderQueueSize = static_cast<uint32_t>(networkConfig.senderQueueSize.getValue()),
                .maxPendingAcks = static_cast<uint32_t>(networkConfig.maxPendingAcks.getValue()),
                .receiverQueueSize = static_cast<uint32_t>(networkConfig.receiverQueueSize.getValue()),
                .senderIOThreads = static_cast<uint32_t>(networkConfig.senderIOThreads.getValue()),
                .receiverIOThreads = static_cast<uint32_t>(networkConfig.receiverIOThreads.getValue()),
            });
    }
}

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) noexcept
{
    CPPTRACE_TRY
    {
        /// Check if the plan already has a local query ID, generate one if needed
        /// but preserve the distributed query ID if present
        if (plan.getQueryId().getLocalQueryId() == INVALID_LOCAL_QUERY_ID)
        {
            auto localId = LocalQueryId(generateUUID());
            if (plan.getQueryId().isDistributed())
            {
                plan.setQueryId(QueryId::create(localId, plan.getQueryId().getDistributedQueryId()));
            }
            else
            {
                plan.setQueryId(QueryId::createLocal(localId));
            }
        }

        const LogContext context("queryId", plan.getQueryId());

        listener->onEvent(SubmitQuerySystemEvent{plan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        const DumpMode dumpMode(
            configuration.workerConfiguration.dumpQueryCompilationIR.getValue(), configuration.workerConfiguration.dumpGraph.getValue());
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(plan);
        request->dumpCompilationResult = dumpMode;
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successful query compilation or exception, but got nothing");
        result->priority = plan.getPriority();
        nodeEngine->registerCompiledQueryPlan(plan.getQueryId(), std::move(result));
        return plan.getQueryId();
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<QueryId, Exception> SingleNodeWorker::registerQueryDeferred(LogicalPlan plan) noexcept
{
    CPPTRACE_TRY
    {
        if (plan.getQueryId().getLocalQueryId() == INVALID_LOCAL_QUERY_ID)
        {
            auto localId = LocalQueryId(generateUUID());
            if (plan.getQueryId().isDistributed())
            {
                plan.setQueryId(QueryId::create(localId, plan.getQueryId().getDistributedQueryId()));
            }
            else
            {
                plan.setQueryId(QueryId::createLocal(localId));
            }
        }

        const LogContext context("queryId", plan.getQueryId());
        listener->onEvent(SubmitQuerySystemEvent{plan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        const DumpMode dumpMode(
            configuration.workerConfiguration.dumpQueryCompilationIR.getValue(), configuration.workerConfiguration.dumpGraph.getValue());
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(plan);
        request->dumpCompilationResult = dumpMode;
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successful query compilation or exception, but got nothing");

        auto pending = std::make_shared<PendingPlan>(PendingPlan{.qep = std::move(result), .dataLogicalPlan = plan});
        const auto qid = plan.getQueryId();
        pendingPlans->withWLock([&](auto& map) { map.emplace(qid, std::move(pending)); });
        return qid;
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

namespace
{
/// Collect the ExecutablePipelines reachable from `sources` in source-to-sink BFS order, returning
/// only the intermediate ("compiled") stages whose stage class is CompiledExecutablePipelineStage.
/// Sinks are skipped — their stage type is sink-specific (FileSink, GrpcSink, ...) and not paired
/// for switching. The ordering is deterministic given a deterministic compiler: each chain visits
/// pipelines in the same relative order, so two structurally-identical plans line up element-wise.
std::vector<std::shared_ptr<ExecutablePipeline>>
collectIntermediatePipelines(const std::vector<CompiledQueryPlan::Source>& sources)
{
    std::vector<std::shared_ptr<ExecutablePipeline>> result;
    std::unordered_set<PipelineId::Underlying> seen;
    std::queue<std::shared_ptr<ExecutablePipeline>> queue;
    for (const auto& src : sources)
    {
        for (const auto& succWeak : src.successors)
        {
            if (auto succ = succWeak.lock())
            {
                if (seen.insert(succ->id.getRawValue()).second)
                {
                    queue.push(succ);
                }
            }
        }
    }
    while (not queue.empty())
    {
        auto current = std::move(queue.front());
        queue.pop();
        if (dynamic_cast<const CompiledExecutablePipelineStage*>(current->stage.get()) != nullptr)
        {
            result.push_back(current);
        }
        for (const auto& succWeak : current->successors)
        {
            if (auto succ = succWeak.lock())
            {
                if (seen.insert(succ->id.getRawValue()).second)
                {
                    queue.push(succ);
                }
            }
        }
    }
    return result;
}
}

std::expected<void, Exception> SingleNodeWorker::attachAlternatePipeline(
    QueryId queryId, LogicalPlan alternatePlan, std::string switchName, int64_t alternateExpectedValue) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        PRECONDITION(not switchName.empty(), "switchName must be non-empty");

        std::shared_ptr<PendingPlan> pending;
        pendingPlans->withWLock(
            [&](auto& map)
            {
                if (const auto it = map.find(queryId); it != map.end())
                {
                    pending = it->second;
                    map.erase(it);
                }
            });
        if (not pending)
        {
            throw QueryNotRegistered("attachAlternatePipeline: queryId {} not found in pending plans", queryId);
        }

        /// Compile the alternate plan with the same compiler pipeline as the data plan. We discard
        /// the alternate's sources and sinks afterward; only its intermediate compiled stages are
        /// merged into the data plan via SwitchableCompiledExecutablePipelineStage.
        const DumpMode dumpMode(
            configuration.workerConfiguration.dumpQueryCompilationIR.getValue(), configuration.workerConfiguration.dumpGraph.getValue());
        auto altRequest = std::make_unique<QueryCompilation::QueryCompilationRequest>(alternatePlan);
        altRequest->dumpCompilationResult = dumpMode;
        auto alternateCompiled = compiler->compileQuery(std::move(altRequest));
        INVARIANT(alternateCompiled, "expected successful query compilation of alternate plan");

        auto dataStages = collectIntermediatePipelines(pending->qep->sources);
        auto altStages = collectIntermediatePipelines(alternateCompiled->sources);
        if (dataStages.size() != altStages.size())
        {
            throw NotImplemented(
                "attachAlternatePipeline: data and alternate plans have different number of intermediate pipelines ({} vs {})",
                dataStages.size(),
                altStages.size());
        }

        const auto selector = SwitchRegistry::instance().getOrCreate(switchName, 0);
        for (size_t i = 0; i < dataStages.size(); ++i)
        {
            /// Preserve the firstPipeline flag — the query engine's throughput listener only emits
            /// TaskEmit events for the source's immediate successor (the "first" pipeline). Without
            /// this propagation the SwitchableStage's default `firstPipeline = false` would
            /// silently disable throughput reporting on the data query.
            const bool firstPipeline = dataStages[i]->stage->firstPipeline;
            auto switchable = std::make_unique<SwitchableCompiledExecutablePipelineStage>(
                std::move(dataStages[i]->stage), std::move(altStages[i]->stage), selector, alternateExpectedValue);
            switchable->firstPipeline = firstPipeline;
            dataStages[i]->stage = std::move(switchable);
        }

        nodeEngine->registerCompiledQueryPlan(queryId, std::move(pending->qep));
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::startQuery(QueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->startQuery(queryId);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::stopQuery(QueryId queryId, QueryTerminationType type) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->stopQuery(queryId, type);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<LocalQueryStatusSnapshot, Exception> SingleNodeWorker::getQueryStatus(QueryId queryId) const noexcept
{
    CPPTRACE_TRY
    {
        auto status = nodeEngine->getQueryLog()->getQueryStatus(queryId);
        if (not status.has_value())
        {
            return std::unexpected{QueryNotFound("{}", queryId)};
        }
        return status.value();
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

WorkerStatus SingleNodeWorker::getWorkerStatus(std::chrono::system_clock::time_point after) const
{
    const std::chrono::system_clock::time_point until = std::chrono::system_clock::now();
    const auto summaries = nodeEngine->getQueryLog()->getStatus();
    WorkerStatus status;
    status.after = after;
    status.until = until;
    for (const auto& [queryId, state, metrics] : summaries)
    {
        switch (state)
        {
            case QueryStatus::Registered:
                /// Ignore these for the worker status
                break;
            case QueryStatus::Started:
                INVARIANT(metrics.start.has_value(), "If query is started, it should have a start timestamp");
                if (metrics.start.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, std::nullopt);
                }
                break;
            case QueryStatus::Running: {
                INVARIANT(metrics.running.has_value(), "If query is running, it should have a running timestamp");
                if (metrics.running.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, metrics.running.value());
                }
                break;
            }
            case QueryStatus::Stopped: {
                INVARIANT(metrics.running.has_value(), "If query is stopped, it should have a running timestamp");
                INVARIANT(metrics.stop.has_value(), "If query is stopped, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }
            case QueryStatus::Failed: {
                INVARIANT(metrics.stop.has_value(), "If query has failed, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }
        }
    }
    return status;
}

}
