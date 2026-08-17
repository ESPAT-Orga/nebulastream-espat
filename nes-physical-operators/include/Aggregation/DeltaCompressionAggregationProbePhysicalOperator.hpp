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
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <HashMapOptions.hpp>
#include <WindowProbePhysicalOperator.hpp>

namespace NES
{

/// Variant of AggregationProbePhysicalOperator for reset/lift-based histogram delta compression.
///
/// Identical combine phase, but at the lower step it injects a "baseline" state into the baseline-aware
/// `lower(state, baseline, ...)` — GEN subtracts it to emit a sparse delta, RESOLVER adds it to emit the
/// full histogram. Injecting at lower (after combine) makes this correct for any thread count.
///
/// A keyframe lowers against a zero baseline and publishes its own state as the interval's
/// reference.
class DeltaCompressionAggregationProbePhysicalOperator final : public WindowProbePhysicalOperator
{
public:
    DeltaCompressionAggregationProbePhysicalOperator(
        HashMapOptions hashMapOptions,
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
        OperatorHandlerId operatorHandlerId,
        WindowMetaData windowMetaData,
        uint64_t keyframeInterval,
        bool isResolver);
    nautilus::val<uint64_t> open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    HashMapOptions hashMapOptions;
    /// Sum of all aggregation functions' state sizes = size of one combined per-key state.
    uint64_t totalStateSizeInBytes;
    uint64_t keyframeInterval;
    bool isResolver;
};

}
