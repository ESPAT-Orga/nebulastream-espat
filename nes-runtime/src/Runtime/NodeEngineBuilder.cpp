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

#include <Runtime/NodeEngineBuilder.hpp>

#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <utility>
#include <Configuration/WorkerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategy.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategyFactory.hpp>
#include <Sources/SourceProvider.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <StatisticStore/DefaultStatisticStore.hpp>
#include <StatisticStore/SubStoresStatisticStore.hpp>
#include <StatisticStore/WindowStatisticStore.hpp>
#include <AdaptiveSendingScheduler.hpp>
#include <CapacityEstimator.hpp>
#include <CapacityEstimatorMode.hpp>
#include <ErrorHandling.hpp>
#include <NetworkSinkSendingStrategyType.hpp>
#include <Priority.hpp>
#include <QueryEngine.hpp>

namespace NES
{


NodeEngineBuilder::NodeEngineBuilder(
    const WorkerConfiguration& workerConfiguration,
    std::shared_ptr<StatisticListener> statisticsListener,
    NetworkSinkSendingStrategyType networkSinkSendingStrategy,
    std::shared_ptr<BackpressureStatisticListener> backpressureStatisticListener)
    : workerConfiguration(workerConfiguration)
    , statisticsListener(std::move(statisticsListener))
    , networkSinkSendingStrategy(networkSinkSendingStrategy)
    , backpressureStatisticListener(std::move(backpressureStatisticListener))
{
}

std::unique_ptr<NodeEngine> NodeEngineBuilder::build(const Host& host)
{
    const auto bufferSize = workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue();
    const auto numBuffers = workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue();
    const auto maxInflightBuffers = workerConfiguration.defaultMaxInflightBuffers.getValue();
    const auto numWorkerThreads = workerConfiguration.queryEngine.numberOfWorkerThreads.getValue();
    fprintf(
        stderr,
        "[NodeEngineBuilder] BufferManager: operator_buffer_size=%lu, number_of_buffers_in_global_buffer_manager=%lu, "
        "default_max_inflight_buffers=%lu, number_of_worker_threads=%lu\n",
        static_cast<unsigned long>(bufferSize),
        static_cast<unsigned long>(numBuffers),
        static_cast<unsigned long>(maxInflightBuffers),
        static_cast<unsigned long>(numWorkerThreads));
    fflush(stderr);
    auto bufferManager = BufferManager::create(bufferSize, numBuffers, statisticsListener);
    auto queryLog = std::make_shared<QueryLog>();

    auto queryEngine = std::make_unique<QueryEngine>(workerConfiguration.queryEngine, statisticsListener, queryLog, bufferManager, host);

    auto sourceProvider = std::make_unique<SourceProvider>(workerConfiguration.defaultMaxInflightBuffers.getValue(), bufferManager);

    const auto concurrency = workerConfiguration.queryEngine.numberOfWorkerThreads.getValue();
    std::shared_ptr<AbstractStatisticStore> statisticStore;
    switch (workerConfiguration.statisticStoreType.getValue())
    {
        case StatisticStoreType::DEFAULT:
            statisticStore = std::make_shared<DefaultStatisticStore>();
            break;
        case StatisticStoreType::WINDOW:
            statisticStore = std::make_shared<WindowStatisticStore>(concurrency);
            break;
        case StatisticStoreType::SUB_STORES:
            statisticStore = std::make_shared<SubStoresStatisticStore>(concurrency);
            break;
    }
    INVARIANT(statisticStore != nullptr, "Unhandled StatisticStoreType");

    auto sendingStrategy = createNetworkSinkSendingStrategy(networkSinkSendingStrategy);

    /// Construct the per-worker AdaptiveSendingScheduler unconditionally. It's cheap when no
    /// channels register (the tick is a no-op on an empty registry), so we always have it
    /// available for the WEIGHTED_PRIO strategy. Pick the capacity-estimation strategy from the
    /// worker config: EMA (default) tracks observed throughput; FIXED uses a constant value (the
    /// scheduler_fixed_capacity_bps knob, defaulting to scheduler_bootstrap_capacity_bps when 0).
    const auto bootstrapBps = workerConfiguration.network.schedulerBootstrapCapacityBps.getValue();
    std::shared_ptr<CapacityEstimator> capacityEstimator;
    switch (workerConfiguration.network.schedulerCapacityMode.getValue())
    {
        case CapacityEstimatorMode::EMA:
            /// emaAlpha was historically 0.3 — kept here as the canonical "tracks observed" value.
            /// If a future config exposes it, plumb it through here.
            capacityEstimator = std::make_shared<EmaCapacityEstimator>(bootstrapBps, /*alpha=*/0.3);
            break;
        case CapacityEstimatorMode::FIXED: {
            const auto fixedBps = workerConfiguration.network.schedulerFixedCapacityBps.getValue();
            capacityEstimator = std::make_shared<FixedCapacityEstimator>(fixedBps > 0 ? fixedBps : bootstrapBps);
            break;
        }
    }
    AdaptiveSendingScheduler::SchedulerConfig schedulerConfig{
        .tickPeriod = std::chrono::milliseconds{workerConfiguration.network.schedulerTickMs.getValue()},
        .priorityWeights
        = {{Priority::HIGH, workerConfiguration.network.schedulerHighWeight.getValue()},
           {Priority::LOW, workerConfiguration.network.schedulerLowWeight.getValue()}},
        .burstCapPerChannelBytes = workerConfiguration.network.schedulerBurstCapBytes.getValue(),
        .debugLog = workerConfiguration.network.schedulerDebugLog.getValue(),
        .capacityEstimator = std::move(capacityEstimator),
    };
    auto adaptiveSendingScheduler = std::make_shared<AdaptiveSendingScheduler>(std::move(schedulerConfig));
    adaptiveSendingScheduler->start();

    return std::make_unique<NodeEngine>(
        std::move(bufferManager),
        statisticsListener,
        std::move(queryLog),
        std::move(queryEngine),
        std::move(sourceProvider),
        std::move(statisticStore),
        std::move(sendingStrategy),
        backpressureStatisticListener,
        std::move(adaptiveSendingScheduler));
}

}
