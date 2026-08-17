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
#include <Aggregation/AggregationOperatorHandler.hpp>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include <Aggregation/AggregationSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

AggregationOperatorHandler::AggregationOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const uint64_t maxNumberOfBuckets)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    , setupAlreadyCalled(false)
    , rollingAverageNumberOfKeys(RollingAverage<uint64_t>{100})
    , maxNumberOfBuckets(maxNumberOfBuckets)
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
AggregationOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Was setWorkerThreads() being called?");
    auto newHashMapArgs = dynamic_cast<const CreateNewHashMapSliceArgs&>(newSlicesArguments);
    newHashMapArgs.numberOfBuckets = std::clamp(rollingAverageNumberOfKeys.rlock()->getAverage(), 1UL, maxNumberOfBuckets);
    return std::function(
        [outputOriginId = outputOriginId, numberOfWorkerThreads = numberOfWorkerThreads, copyOfNewHashMapArgs = newHashMapArgs](
            SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new aggregation slice with for slice {}-{} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            return {std::make_shared<AggregationSlice>(sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfWorkerThreads)};
        });
}

void AggregationOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        /// Getting all hashmaps for each slice that has at least one tuple
        std::unique_ptr<ChainedHashMap> finalHashMap;
        std::vector<HashMap*> allHashMaps;
        uint64_t totalNumberOfTuples = 0;
        for (const auto& slice : allSlices)
        {
            const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(slice);
            for (uint64_t hashMapIdx = 0; hashMapIdx < aggregationSlice->getNumberOfHashMaps(); ++hashMapIdx)
            {
                if (auto* hashMap = aggregationSlice->getHashMapPtr(WorkerThreadId(hashMapIdx));
                    (hashMap != nullptr) and hashMap->getNumberOfTuples() > 0)
                {
                    /// As the hashmap has one value per key, we can use the number of tuples for the number of keys
                    rollingAverageNumberOfKeys.wlock()->add(hashMap->getNumberOfTuples());

                    /// We store here the raw pointer, as we need the raw pointers to operate over them in the AggregationProbe
                    allHashMaps.emplace_back(hashMap);
                    totalNumberOfTuples += hashMap->getNumberOfTuples();
                    if (not finalHashMap)
                    {
                        finalHashMap = ChainedHashMap::createNewMapWithSameConfiguration(*dynamic_cast<ChainedHashMap*>(hashMap));
                    }
                }
            }
        }


        /// We need a buffer that is large enough to store:
        /// - all pointers to all hashmaps of the window to be triggered
        /// - a new hashmap for the probe operator, so that we are not overwriting the thread local hashmaps
        /// - size of EmittedAggregationWindow
        const auto neededBufferSize = sizeof(EmittedAggregationWindow) + (allHashMaps.size() * sizeof(HashMap*));
        const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
        if (not tupleBufferVal.has_value())
        {
            throw CannotAllocateBuffer("{}B for the hash join window trigger were requested", neededBufferSize);
        }
        auto tupleBuffer = tupleBufferVal.value();

        /// It might be that the buffer is not zeroed out.
        std::ranges::fill(tupleBuffer.getAvailableMemoryArea(), std::byte{0});

        /// As we are here "emitting" a buffer, we have to set the originId, the seq number, the watermark and the "number of tuples".
        /// The watermark cannot be the slice end as some buffers might be still waiting to get processed.
        tupleBuffer.setOriginId(outputOriginId);
        tupleBuffer.setSequenceNumber(windowInfo.sequenceNumber);
        tupleBuffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
        tupleBuffer.setLastChunk(true);
        tupleBuffer.setWatermark(windowInfo.windowInfo.windowStart);
        tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
        tupleBuffer.setCreationTimestampInMS(Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));


        /// Writing all necessary information for the aggregation probe to the buffer via the placement new constructor
        auto tmp = tupleBuffer.getAvailableMemoryArea();
        new (tmp.data()) EmittedAggregationWindow{windowInfo.windowInfo, std::move(finalHashMap), allHashMaps};


        /// Dispatching the buffer to the probe operator via the task queue.
        pipelineCtx->emitBuffer(tupleBuffer);
        NES_TRACE(
            "Emitted window {}-{} with watermarkTs {} sequenceNumber {} originId {}",
            windowInfo.windowInfo.windowStart,
            windowInfo.windowInfo.windowEnd,
            tupleBuffer.getWatermark(),
            tupleBuffer.getSequenceNumber(),
            tupleBuffer.getOriginId());
    }
}

namespace
{
/// Records the eviction threshold for `interval` (running max windowEnd). Caller must hold keyframeMutex.
void recordIntervalEnd(std::map<uint64_t, uint64_t>& endMap, const uint64_t interval, const uint64_t windowEnd)
{
    auto& endTs = endMap[interval];
    endTs = std::max(endTs, windowEnd);
}
}

int8_t* AggregationOperatorHandler::zeroBaselineFor(const uint64_t stateSize)
{
    const std::lock_guard lock{keyframeMutex};
    if (zeroBaseline.empty())
    {
        zeroBaseline.assign(stateSize, 0);
    }
    /// Sized once, by the first keyframe. The returned pointer is used by the caller AFTER keyframeMutex is
    /// released, so re-assigning on a differing stateSize could reallocate the buffer under a concurrent reader
    /// still holding a pointer from an earlier call. One handler serves one state size, so a mismatch is a bug
    /// to surface rather than a case to accommodate.
    INVARIANT(
        zeroBaseline.size() == stateSize,
        "Zero baseline was sized {} but a keyframe requested {}; one handler must serve a single state size",
        zeroBaseline.size(),
        stateSize);
    return zeroBaseline.data();
}

int8_t* AggregationOperatorHandler::tryGetKeyframeBaseline(const uint64_t interval, const uint64_t windowEnd)
{
    const std::lock_guard lock{keyframeMutex};
    /// Every window advances the interval's eviction threshold to the max windowEnd seen so far. This is safe
    /// for onGarbageCollect despite being a running max: the contiguous probe watermark is held below the start
    /// of any still-unprocessed window, so it cannot exceed the recorded max (>= a processed window's end > its
    /// start). The max becomes the true interval end once the interval's last window has been processed.
    recordIntervalEnd(keyframeIntervalEndTs, interval, windowEnd);
    if (const auto it = keyframeReferences.find(interval); it != keyframeReferences.end())
    {
        /// Return a COPY in a thread-local scratch, so onGarbageCollect can evict concurrently without dangling
        /// this reader.
        thread_local std::vector<int8_t> baselineScratch;
        baselineScratch.assign(it->second.begin(), it->second.end());
        return baselineScratch.data();
    }
    return nullptr;
}

bool AggregationOperatorHandler::isKeyframeReady(const uint64_t interval) const
{
    const std::lock_guard lock{keyframeMutex};
    return keyframeReferences.contains(interval);
}

void AggregationOperatorHandler::publishKeyframe(
    const uint64_t interval, const int8_t* state, const uint64_t stateSize, const uint64_t windowEnd)
{
    const std::lock_guard lock{keyframeMutex};
    recordIntervalEnd(keyframeIntervalEndTs, interval, windowEnd);
    auto& reference = keyframeReferences[interval];
    reference.assign(state, state + stateSize);
}

void AggregationOperatorHandler::onGarbageCollect(const Timestamp newGlobalWatermark) const
{
    /// An interval entirely below the probe's new global watermark has had all of its windows probed, so no
    /// future delta can reference its keyframe. Riding the same watermark that deletes the slices and windows
    /// is what makes that safe.
    const std::lock_guard lock{keyframeMutex};
    for (auto it = keyframeIntervalEndTs.begin(); it != keyframeIntervalEndTs.end();)
    {
        const auto [interval, intervalEndTs] = *it;
        if (Timestamp(intervalEndTs) < newGlobalWatermark)
        {
            keyframeReferences.erase(interval);
            it = keyframeIntervalEndTs.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

}
