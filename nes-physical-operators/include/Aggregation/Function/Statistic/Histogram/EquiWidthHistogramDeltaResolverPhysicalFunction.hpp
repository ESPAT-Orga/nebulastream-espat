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
#include <Aggregation/Function/Statistic/Histogram/EquiWidthHistogramPhysicalFunction.hpp>

namespace NES
{
/// Node-2 (store owner) side of reset/lift-based histogram delta compression.
///
/// `reset` is the standard zero+bounds reset. `lift` does NOT consume raw data tuples: it consumes one
/// incoming sparse-delta blob record per window (produced by EquiWidthHistogramDeltaGenPhysicalFunction)
/// and adds the per-bin deltas onto the (zeroed) state. The baseline-aware `lower` then adds the baseline
/// the probe supplies and serialises the FULL histogram for the store writer.
///
/// `inputFunction` must be a field access to the incoming delta blob field (set during lowering).
class EquiWidthHistogramDeltaResolverPhysicalFunction final : public EquiWidthHistogramPhysicalFunction
{
public:
    using EquiWidthHistogramPhysicalFunction::EquiWidthHistogramPhysicalFunction;

    /// Apply the incoming sparse-delta blob onto the (zeroed) state, and record its keyframe flag (bit 63 of
    /// the blob's first header word) into the trailing flag byte of the state (see getSizeOfStateInBytes).
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;

    /// Standard full-blob lower (fallback when no baseline is available).
    using EquiWidthHistogramPhysicalFunction::lower;

    /// Add the baseline onto the state, then emit the full histogram. `isKeyframe` and `intervalId` are unused
    /// here: the probe already selected the right baseline (zero for a keyframe, the interval reference
    /// otherwise) and reads the interval id from the state, where lift stored it.
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<AggregationState*> baselineState,
        const nautilus::val<bool>& isKeyframe,
        const nautilus::val<uint64_t>& intervalId,
        PipelineMemoryProvider& pipelineMemoryProvider) override;

    /// The resolver state is the histogram plus two trailing 8-byte words carrying this window's keyframe flag
    /// and keyframe-interval id (both set by lift from the wire), which the delta-compression probe reads to
    /// pick the baseline and its cache key. reset zeroes them; combine ORs the flag and takes the max interval
    /// id so both survive the probe's per-thread-hashmap merge; the base `lower` never serializes them (it uses
    /// the pure histogramStateBytes()), so the stored blob is unchanged.
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;

    /// Byte offset of the keyframe flag word within the state (== the pure histogram size).
    [[nodiscard]] size_t keyframeFlagOffset() const { return histogramStateBytes(); }

    /// Byte offset of the keyframe-interval id word within the state (right after the flag word).
    [[nodiscard]] size_t intervalIdOffset() const { return histogramStateBytes() + sizeof(uint64_t); }
};
}
