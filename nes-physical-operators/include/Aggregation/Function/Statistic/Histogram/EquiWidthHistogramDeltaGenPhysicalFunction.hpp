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
#include <DataTypes/DataType.hpp>

namespace NES
{
/// Node-1 (builder) side of reset/lift-based histogram delta compression.
///
/// Build (`reset`/`lift`/`combine`) is identical to the standard equi-width histogram: the current
/// window is accumulated normally. The difference is at `lower`: instead of emitting the full
/// histogram, we emit a SPARSE DELTA against the baseline the delta-aware probe operator supplies (the
/// interval's keyframe state, or zero for a keyframe window). Only bins whose counter changed are
/// emitted, and bin bounds are never emitted (node 2 knows them from the query).
///
/// Sparse delta blob layout (consumed only by EquiWidthHistogramDeltaResolverPhysicalFunction::lift):
///   [uint64 numChangedBinsAndFlag][uint64 nTuplesDelta][uint64 intervalId]   /// 24-byte header
///   [ { uint64 binIndex, <counterType> counterDelta } * numChangedBins ]
/// Bit 63 of the first header word is the KEYFRAME flag; its low 63 bits are numChangedBins (bounded by
/// numberOfBins, so 63 bits is ample). The third word carries this window's keyframe-interval id, which the
/// RESOLVER uses to group deltas with their keyframe. See DELTA_HEADER_SIZE / KEYFRAME_FLAG_BIT in the .cpp.
/// The total byte size is carried out-of-band by the VariableSizedData wrapper, so it is NOT stored in-band.
class EquiWidthHistogramDeltaGenPhysicalFunction final : public EquiWidthHistogramPhysicalFunction
{
public:
    using EquiWidthHistogramPhysicalFunction::EquiWidthHistogramPhysicalFunction;

    /// Standard full-blob lower (used as a fallback when no baseline is available).
    using EquiWidthHistogramPhysicalFunction::lower;

    /// Emit the sparse delta (current - baseline). `isKeyframe` and `intervalId` are stamped into the blob
    /// header so the RESOLVER can tell a keyframe from a delta and group deltas with their keyframe by GEN's
    /// grouping rather than recomputing it.
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<AggregationState*> baselineState,
        const nautilus::val<bool>& isKeyframe,
        const nautilus::val<uint64_t>& intervalId,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
};
}
