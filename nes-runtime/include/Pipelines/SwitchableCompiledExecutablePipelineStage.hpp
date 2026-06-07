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

#include <atomic>
#include <cstdint>
#include <memory>
#include <ostream>
#include <Runtime/TupleBuffer.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Holds two underlying pipeline stages and dispatches each incoming buffer to one of them
/// based on a runtime-readable switch atomic. Built by the worker when an alternate pipeline
/// is attached to a deployed query (gRPC AttachAlternatePipeline + the workload-switch design):
/// the data query's filter chain becomes the `primary` and the parsed alternate's matching
/// stage becomes the `alternate`. Both compiled functions live side by side in the same
/// ExecutablePipeline; the source thread and the downstream sink stay unchanged.
///
/// The dispatch is one relaxed atomic load + an indirect call per buffer — essentially the
/// same hot path as a plain CompiledExecutablePipelineStage. Stages must agree on input and
/// output schemas; mismatched schemas would corrupt downstream pipelines and are the caller's
/// responsibility to verify (see worker-side stage matching in AttachAlternatePipeline).
class SwitchableCompiledExecutablePipelineStage final : public ExecutablePipelineStage
{
public:
    SwitchableCompiledExecutablePipelineStage(
        std::unique_ptr<ExecutablePipelineStage> primary,
        std::unique_ptr<ExecutablePipelineStage> alternate,
        std::shared_ptr<std::atomic<int64_t>> selector,
        int64_t alternateExpectedValue);

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    std::unique_ptr<ExecutablePipelineStage> primary;
    std::unique_ptr<ExecutablePipelineStage> alternate;
    std::shared_ptr<std::atomic<int64_t>> selector;
    int64_t alternateExpectedValue;
};

}
