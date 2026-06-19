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
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategy.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Logger/Formatter.hpp>
#include <CompiledQueryPlan.hpp>
#include <QueryId.hpp>

namespace NES
{
struct BackpressureStatisticListener;
class AdaptiveSendingScheduler;

/// The ExecutableQueryPlan represents a query with completely instantiated query processing components (Sources, Pipelines, Sinks).
/// In this form the Query could be executed, by starting all pipelines, sinks and passing the successor pipelines into the queries sources.
struct ExecutableQueryPlan
{
    /// One source entry. In the normal case `source` is a freshly-built SourceHandle that the
    /// engine starts on a dedicated thread. When `spliceToRunningSource == true`, `source` is
    /// nullptr: the engine must NOT spawn a thread but instead look up the running source for
    /// `logicalSourceName` in the worker-wide RunningSourceRegistry and graft this entry's
    /// `successors` onto it. Strict: missing match → fail.
    struct SourceWithSuccessor
    {
        std::unique_ptr<SourceHandle> source;
        std::vector<std::weak_ptr<ExecutablePipeline>> successors;
        bool spliceToRunningSource = false;
        /// If true, the source is created and registered in RunningSourceRegistry but NOT
        /// started until `deferStartExpectedSpliceCount` successful appendSuccessors() calls
        /// have happened (or until an explicit startDeferred(name) call).
        bool deferStart = false;
        uint32_t deferStartExpectedSpliceCount = 1;
        std::string logicalSourceName;
    };

    /// *backpressureStatisticListener* (optional) is wired into both the controller and listener of the
    /// freshly created backpressure channel so NetworkSink emits BufferSentEvents and the source side
    /// emits BufferIngestEvents on every TupleBuffer.
    /// *adaptiveSendingScheduler* (optional) is registered with the controller so the WEIGHTED_PRIO
    /// sending strategy can gate sends through per-channel contingents. Null when the strategy
    /// doesn't need it.
    static std::unique_ptr<ExecutableQueryPlan> instantiate(
        CompiledQueryPlan& compiledQueryPlan,
        const SourceProvider& sourceProvider,
        std::shared_ptr<NetworkSinkSendingStrategy> sendingStrategy,
        std::shared_ptr<BackpressureStatisticListener> backpressureStatisticListener = nullptr,
        std::shared_ptr<AdaptiveSendingScheduler> adaptiveSendingScheduler = nullptr);
    ExecutableQueryPlan(
        QueryId queryId, std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<SourceWithSuccessor> instantiatedSources);

    QueryId queryId;
    std::vector<std::shared_ptr<ExecutablePipeline>> pipelines;
    std::vector<SourceWithSuccessor> sources;
    friend std::ostream& operator<<(std::ostream& os, const ExecutableQueryPlan& executableQueryPlan);
};
}

FMT_OSTREAM(NES::ExecutableQueryPlan);
