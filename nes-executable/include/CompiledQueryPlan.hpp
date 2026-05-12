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
#include <string>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ExecutablePipelineStage.hpp>
#include <Priority.hpp>
#include <QueryId.hpp>

namespace NES
{

struct ExecutablePipeline
{
    static std::shared_ptr<ExecutablePipeline> create(
        PipelineId id, std::unique_ptr<ExecutablePipelineStage> stage, const std::vector<std::shared_ptr<ExecutablePipeline>>& successors);

    PipelineId id;
    std::unique_ptr<ExecutablePipelineStage> stage;
    std::vector<std::weak_ptr<ExecutablePipeline>> successors;
};

struct CompiledQueryPlan
{
    struct Source
    {
        /// The Source representation in the `CompiledQueryPlan` is still an abstract source representation. During Query Instantiation
        /// the descriptor and originId are instantiated into concrete source implementation.
        OriginId originId;
        PipelineId pipelineId;
        OperatorId operatorId;
        SourceDescriptor descriptor;

        /// Sources do not have any predecessors
        std::vector<std::weak_ptr<ExecutablePipeline>> successors;

        /// If true, the query lowering tagged this source with SpliceToRunningSourceTrait. At
        /// instantiation time the engine MUST NOT spawn a source thread; instead it looks up the
        /// already-running source for `logicalSourceName` and grafts this entry's successors onto
        /// it. logicalSourceName is taken from descriptor.getLogicalSource() at lowering time and
        /// pinned here so the runtime path doesn't depend on the descriptor still being live.
        bool spliceToRunningSource = false;
        /// If true, the runtime creates the RunningSource and registers it but does NOT start
        /// its emit thread until `deferStartExpectedSpliceCount` successful appendSuccessors()
        /// calls have happened (or until an explicit RunningSourceRegistry::startDeferred(name)).
        bool deferStart = false;
        uint32_t deferStartExpectedSpliceCount = 1;
        std::string logicalSourceName;
    };

    struct Sink
    {
        PipelineId id;
        /// The Sink representation in the `CompiledQueryPlan` is still an abstract sink representation. During Query Instantiation
        /// the descriptor is instantiated into concrete sink implementation.
        SinkDescriptor descriptor;

        /// Sinks do not have any successors
        std::vector<std::variant<OperatorId, std::weak_ptr<ExecutablePipeline>>> predecessor;
    };

    static std::unique_ptr<CompiledQueryPlan> create(
        QueryId queryId, std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<Sink> sinks, std::vector<Source> sources);

    QueryId queryId;
    std::vector<std::shared_ptr<ExecutablePipeline>> pipelines;
    std::vector<Sink> sinks;
    std::vector<Source> sources;
    Priority priority = Priority::HIGH;
};
}
