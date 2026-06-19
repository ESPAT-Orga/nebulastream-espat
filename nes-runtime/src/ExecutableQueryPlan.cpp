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

#include <ExecutableQueryPlan.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategy.hpp>
#include <Sinks/SinkProvider.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Overloaded.hpp>
#include <AdaptiveSendingScheduler.hpp>
#include <BackpressureChannel.hpp>
#include <BackpressureStatisticsListener.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& os, const ExecutableQueryPlan& instantiatedQueryPlan)
{
    std::function<void(const std::weak_ptr<ExecutablePipeline>&, size_t)> printNode
        = [&os, &printNode](const std::weak_ptr<ExecutablePipeline>& weakPipeline, size_t indent)
    {
        auto pipeline = weakPipeline.lock();
        os << std::string(indent * 4, ' ') << *pipeline->stage << "(" << pipeline->id << ")" << '\n';
        for (const auto& successor : pipeline->successors)
        {
            printNode(successor, indent + 1);
        }
    };

    for (const auto& entry : instantiatedQueryPlan.sources)
    {
        if (entry.spliceToRunningSource)
        {
            os << "<splice into running source: '" << entry.logicalSourceName << "'>\n";
        }
        else if (entry.source)
        {
            os << *entry.source << '\n';
        }
        else
        {
            os << "<null source>\n";
        }
        for (const auto& successor : entry.successors)
        {
            printNode(successor, 1);
        }
    }
    return os;
}

std::unique_ptr<ExecutableQueryPlan> ExecutableQueryPlan::instantiate(
    CompiledQueryPlan& compiledQueryPlan,
    const SourceProvider& sourceProvider,
    std::shared_ptr<NetworkSinkSendingStrategy> sendingStrategy,
    std::shared_ptr<BackpressureStatisticListener> backpressureStatisticListener,
    std::shared_ptr<AdaptiveSendingScheduler> adaptiveSendingScheduler)
{
    std::vector<SourceWithSuccessor> instantiatedSources;

    std::unordered_map<OperatorId, std::vector<std::shared_ptr<ExecutablePipeline>>> instantiatedSinksWithSourcePredecessor;

    if (compiledQueryPlan.sinks.empty())
    {
        throw NotImplemented("A query plan must declare at least one sink");
    }

    /// One backpressure channel per sink. The merged listener handed to each source aggregates the
    /// signals from every sink, so the source blocks if ANY sink has applied pressure.
    auto [firstController, mergedListener] = createBackpressureChannel();
    std::vector<BackpressureController> sinkControllers;
    sinkControllers.reserve(compiledQueryPlan.sinks.size());
    sinkControllers.push_back(std::move(firstController));
    for (size_t i = 1; i < compiledQueryPlan.sinks.size(); ++i)
    {
        auto [ctrl, lst] = createBackpressureChannel();
        /// Wire both ends of the freshly-created channel to the (optional) statistic listener so events
        /// downstream (NetworkSink::recordBufferSent, BackpressureListener::recordBufferIngested,
        /// applyPressure / releasePressure) carry this query's identity.
        if (backpressureStatisticListener)
        {
            ctrl.setStatisticListener(backpressureStatisticListener, compiledQueryPlan.queryId, compiledQueryPlan.priority);
            ctrl.setStatisticListener(backpressureStatisticListener, compiledQueryPlan.queryId, compiledQueryPlan.priority);
        }

        /// Wire the controller to the (optional) per-worker AdaptiveSendingScheduler so the
        /// WEIGHTED_PRIO sending strategy can gate sends through per-channel contingents. The
        /// scheduler tick will start allocating shares for this channel on its next tick.
        if (adaptiveSendingScheduler)
        {
            ctrl.registerWithScheduler(adaptiveSendingScheduler, compiledQueryPlan.queryId, compiledQueryPlan.priority);
        }
        sinkControllers.push_back(std::move(ctrl));
        mergedListener.merge(std::move(lst));
    }

    for (size_t i = 0; i < compiledQueryPlan.sinks.size(); ++i)
    {
        auto& [sinkPipelineId, sinkDescriptor, predecessors] = compiledQueryPlan.sinks[i];
        auto sink = ExecutablePipeline::create(sinkPipelineId, lower(std::move(sinkControllers[i]), sinkDescriptor, compiledQueryPlan.queryId, compiledQueryPlan.priority, sendingStrategy), {});
        compiledQueryPlan.pipelines.push_back(sink);
        for (const auto& predecessor : predecessors)
        {
            std::visit(
                Overloaded{
                    [&](const OperatorId& source) { instantiatedSinksWithSourcePredecessor[source].push_back(sink); },
                    [&](const std::weak_ptr<ExecutablePipeline>& pipeline) { pipeline.lock()->successors.push_back(sink); },
                },
                predecessor);
        }
    }

    for (auto& compiledSource : compiledQueryPlan.sources)
    {
        std::ranges::copy(
            instantiatedSinksWithSourcePredecessor[compiledSource.operatorId], std::back_inserter(compiledSource.successors));
        if (compiledSource.spliceToRunningSource)
        {
            /// Defer the registry lookup to RunningQueryPlan::start where our RunningQueryPlanNodes
            /// have been created. Here we only flag the entry; the runtime fan-out happens later.
            instantiatedSources.emplace_back(ExecutableQueryPlan::SourceWithSuccessor{
                .source = nullptr,
                .successors = std::move(compiledSource.successors),
                .spliceToRunningSource = true,
                .logicalSourceName = compiledSource.logicalSourceName});
        }
        else
        {
            instantiatedSources.emplace_back(ExecutableQueryPlan::SourceWithSuccessor{
                .source = sourceProvider.lower(compiledSource.originId, compiledSource.pipelineId, mergedListener, compiledSource.descriptor),
                .successors = std::move(compiledSource.successors),
                .spliceToRunningSource = false,
                .deferStart = compiledSource.deferStart,
                .deferStartExpectedSpliceCount = compiledSource.deferStartExpectedSpliceCount,
                .logicalSourceName = compiledSource.logicalSourceName});
        }
    }


    return std::make_unique<ExecutableQueryPlan>(compiledQueryPlan.queryId, compiledQueryPlan.pipelines, std::move(instantiatedSources));
}

ExecutableQueryPlan::ExecutableQueryPlan(
    QueryId queryId, std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<SourceWithSuccessor> instantiatedSources)
    : queryId(queryId), pipelines(std::move(pipelines)), sources(std::move(instantiatedSources))
{
}
}
