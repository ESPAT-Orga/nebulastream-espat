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

#include <memory>
#include <utility>
#include <Configuration/WorkerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/SourceProvider.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <StatisticStore/DefaultStatisticStore.hpp>
#include <StatisticStore/SubStoresStatisticStore.hpp>
#include <StatisticStore/WindowStatisticStore.hpp>
#include <ErrorHandling.hpp>
#include <QueryEngine.hpp>

namespace NES
{


NodeEngineBuilder::NodeEngineBuilder(const WorkerConfiguration& workerConfiguration, std::shared_ptr<StatisticListener> statisticsListener)
    : workerConfiguration(workerConfiguration), statisticsListener(std::move(statisticsListener))
{
}

std::unique_ptr<NodeEngine> NodeEngineBuilder::build(const Host& host)
{
    auto bufferManager = BufferManager::create(
        workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue(),
        workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue(),
        statisticsListener);
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

    return std::make_unique<NodeEngine>(
        std::move(bufferManager),
        statisticsListener,
        std::move(queryLog),
        std::move(queryEngine),
        std::move(sourceProvider),
        std::move(statisticStore));
}

}
