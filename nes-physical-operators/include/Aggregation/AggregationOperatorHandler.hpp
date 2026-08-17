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
#include <algorithm>
#include <bit>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/RollingAverage.hpp>
#include <HashMapSlice.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// This struct models the information for an aggregation window trigger
/// As we are triggering the probe pipeline by passing a tuple buffer to the probe operator, we assume that the tuple buffer
/// is large enough to store all slices of the window to be triggered.
struct EmittedAggregationWindow
{
    EmittedAggregationWindow(const WindowInfo windowInfo, std::unique_ptr<HashMap> finalHashMap, const std::vector<HashMap*>& allHashMaps)
        : windowInfo(windowInfo), finalHashMap(std::move(finalHashMap)), numberOfHashMaps(allHashMaps.size())
    {
        finalHashMapPtr = this->finalHashMap.get();
        /// Copying the hashmap pointers after this object, hence this + 1
        hashMaps = std::bit_cast<HashMap**>(this + 1);
        std::ranges::copy(allHashMaps, std::bit_cast<HashMap**>(hashMaps));
    }

    WindowInfo windowInfo;
    HashMap* finalHashMapPtr;
    std::unique_ptr<HashMap> finalHashMap; /// Pointer to the final hash map that the probe should use to combine all hash maps
    uint64_t numberOfHashMaps;
    HashMap** hashMaps; /// Pointer to the stored pointers of all hash maps that the probe should combine
};

class AggregationOperatorHandler final : public WindowBasedOperatorHandler
{
public:
    AggregationOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        uint64_t maxNumberOfBuckets);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const override;

    /// Is required to not perform the setup again and resolving a race condition to the cleanup state function
    std::atomic<bool> setupAlreadyCalled;
    /// shared_ptr as multiple slices need access to it
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> cleanupStateNautilusFunction;

    /// --- Keyframe reference cache for histogram delta compression ---
    /// Delta compression groups windows into intervals of K consecutive window ordinals. Each interval's
    /// keyframe (its `ordinal % K == 0` window) emits a full histogram and publishes its state here; the
    /// interval's other windows are deltas against that one reference. See
    /// DeltaCompressionAggregationProbePhysicalOperator for how a window learns which of the two it is.
    ///
    /// Handing the reference over is WAIT-FREE: a worker never blocks on a keyframe. The engine does not
    /// process window-tasks in enqueue order, so a keyframe can be dequeued after its deltas; if deltas
    /// blocked on it, all workers could end up blocked on an interval whose keyframe is still queued — a
    /// thread-pool starvation deadlock. A delta that finds no reference reschedules its task instead.

    /// Keyframe window: baseline is zero (=> emit the full histogram). Returns a shared zeroed buffer.
    [[nodiscard]] int8_t* zeroBaselineFor(uint64_t stateSize);
    /// Delta window: if `interval`'s keyframe reference is published, copy it into a thread-local scratch and
    /// return it (the caller reconstructs and emits this delta now). Otherwise return nullptr — the caller
    /// reschedules its task and retries. Also advances the interval's windowEnd eviction threshold.
    [[nodiscard]] int8_t* tryGetKeyframeBaseline(uint64_t interval, uint64_t windowEnd);
    /// Non-mutating readiness probe used to decide (BEFORE the expensive per-thread hash-map combine) whether a
    /// delta window can proceed: true iff `interval`'s keyframe reference is already published. A delta whose
    /// keyframe is not ready reschedules its task pre-combine, so the combine is never re-run on a retry.
    [[nodiscard]] bool isKeyframeReady(uint64_t interval) const;
    /// Keyframe window: publish its post-lower state as `interval`'s reference and record windowEnd for eviction.
    void publishKeyframe(uint64_t interval, const int8_t* state, uint64_t stateSize, uint64_t windowEnd);

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;
    /// Evict keyframe references for intervals whose last window ended before the probe's new global
    /// watermark — they can never be referenced again. Hooked into the engine's watermark-driven GC.
    void onGarbageCollect(Timestamp newGlobalWatermark) const override;
    folly::Synchronized<RollingAverage<uint64_t>> rollingAverageNumberOfKeys;
    uint64_t maxNumberOfBuckets;

private:
    /// interval index -> keyframe window's reference state. Evicted by onGarbageCollect once the watermark
    /// passes the interval (see keyframeIntervalEndTs); readers get a copy, so eviction cannot dangle them.
    mutable std::map<uint64_t, std::vector<int8_t>> keyframeReferences;
    /// interval index -> running max windowEnd over the interval's processed windows (the eviction threshold;
    /// the true interval end once its last window has been processed). See onGarbageCollect.
    mutable std::map<uint64_t, uint64_t> keyframeIntervalEndTs;
    /// Shared read-only zero baseline handed to keyframe windows (sized once; histogram state size is fixed).
    mutable std::vector<int8_t> zeroBaseline;
    mutable std::mutex keyframeMutex;
};

}
