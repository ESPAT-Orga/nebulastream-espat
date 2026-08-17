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
#include <Aggregation/DeltaCompressionAggregationProbePhysicalOperator.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Backoff before a delta window that is waiting on its interval keyframe is re-submitted.
///
/// It must be non-zero. `TaskQueue::readElementAssumingItExists` drains the internal queue completely before it
/// ever reads the admission queue, and source data arrives via the admission queue while a repeated task is
/// re-submitted into the internal one. Repeating at 0 ms therefore recycles inside the high-priority queue and
/// can hold off the very source progress that produces the keyframe being waited for. Mirrors
/// `NetworkSink::BACKPRESSURE_RETRY_INTERVAL`; kept smaller because a keyframe is typically a few tasks away.
constexpr auto KEYFRAME_RETRY_INTERVAL = std::chrono::milliseconds{1};

AggregationOperatorHandler* asAggHandler(OperatorHandler* operatorHandler)
{
    auto* handler = dynamic_cast<AggregationOperatorHandler*>(operatorHandler);
    PRECONDITION(handler != nullptr, "Expected an AggregationOperatorHandler for delta compression");
    return handler;
}

int8_t* zeroBaselineForProxy(OperatorHandler* operatorHandler, const uint64_t stateSize)
{
    return asAggHandler(operatorHandler)->zeroBaselineFor(stateSize);
}

int8_t* tryGetKeyframeBaselineProxy(OperatorHandler* operatorHandler, const uint64_t interval, const uint64_t windowEnd)
{
    return asAggHandler(operatorHandler)->tryGetKeyframeBaseline(interval, windowEnd);
}

bool isKeyframeReadyProxy(OperatorHandler* operatorHandler, const uint64_t interval)
{
    return asAggHandler(operatorHandler)->isKeyframeReady(interval);
}

void publishKeyframeProxy(
    OperatorHandler* operatorHandler, const uint64_t interval, const int8_t* state, const uint64_t stateSize, const uint64_t windowEnd)
{
    asAggHandler(operatorHandler)->publishKeyframe(interval, state, stateSize, windowEnd);
}

/// Clear the emitted window's final hash map so a rescheduled (REPEATed) task re-combines into an empty map
/// instead of doubling the previous pass's combined state. A no-op on the first pass (the map starts empty).
void clearFinalHashMapProxy(EmittedAggregationWindow* emittedAggregationWindow)
{
    if (auto* finalHashMap = dynamic_cast<ChainedHashMap*>(emittedAggregationWindow->finalHashMapPtr))
    {
        finalHashMap->clear();
    }
}
}

nautilus::val<uint64_t>
DeltaCompressionAggregationProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();

    const auto aggregationWindowRef = static_cast<nautilus::val<EmittedAggregationWindow*>>(recordBuffer.getMemArea());
    const auto numberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::numberOfHashMaps));
    const auto windowInfoRef = getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::windowInfo);
    const auto windowStartRaw = readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart));
    const auto windowEndRaw = readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd));
    const nautilus::val<Timestamp> windowStart{windowStartRaw};
    const nautilus::val<Timestamp> windowEnd{windowEndRaw};

    /// Sequence numbers start at SequenceNumber::INITIAL == 1, so shift to a 0-based ordinal: then the interval's
    /// FIRST window (0-based ordinal % K == 0) is its keyframe — the lowest-windowEnd window, enqueued ahead of
    /// its deltas. (Using the raw 1-based number would make ordinal % K == 0 the interval's LAST window, putting
    /// the keyframe behind its deltas, where nothing guarantees it is ever reached.)
    const nautilus::val<uint64_t> keyframeIntervalVal{keyframeInterval};
    const auto genOrdinal = executionCtx.sequenceNumber.convertToValue() - nautilus::val<uint64_t>{1};
    const auto genIntervalIndex = genOrdinal / keyframeIntervalVal;
    const auto genIsKeyframe = (genOrdinal % keyframeIntervalVal) == nautilus::val<uint64_t>{0};
    auto hashMapRefs = readValueFromMemRef<HashMap**>(getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::hashMaps));
    auto finalHashMapPtr = readValueFromMemRef<HashMap*>(getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::finalHashMapPtr));

    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const nautilus::val<uint64_t> totalStateSize{totalStateSizeInBytes};

    /// PRE-COMBINE reschedule gate. A delta window whose interval keyframe is not yet published must reschedule
    /// BEFORE the expensive per-thread hash-map combine — re-running the combine on every retry (as a post-combine
    /// gate would) churns hash-map buffers and, at scale, wedges the query with all workers idle. So determine
    /// this window's keyframe flag + interval id WITHOUT combining: GEN from its own ordinal; RESOLVER by peeking
    /// the flag/interval words that `lift` stamped into any present source-state entry (same trailing-word offsets
    /// the emit loop reads).
    nautilus::val<bool> gateIsKeyframe{genIsKeyframe};
    nautilus::val<uint64_t> gateInterval{genIntervalIndex};
    nautilus::val<bool> gateHasEntry{not isResolver};
    if (isResolver)
    {
        for (nautilus::val<uint64_t> curHashMap = 0; curHashMap < numberOfHashMaps; ++curHashMap)
        {
            const nautilus::val<HashMap*> peekHashMapPtr = hashMapRefs[curHashMap];
            const ChainedHashMapRef peekMap(
                peekHashMapPtr,
                hashMapOptions.fieldKeys,
                hashMapOptions.fieldValues,
                hashMapOptions.entriesPerPage,
                hashMapOptions.entrySize);
            for (const auto peekEntry : peekMap)
            {
                const ChainedHashMapRef::ChainedEntryRef peekEntryRef(
                    peekEntry, peekHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
                const auto peekStateBase = static_cast<nautilus::val<int8_t*>>(peekEntryRef.getValueMemArea());
                gateIsKeyframe = readValueFromMemRef<uint64_t>(peekStateBase + (totalStateSize - nautilus::val<uint64_t>{16}))
                    != nautilus::val<uint64_t>{0};
                gateInterval = readValueFromMemRef<uint64_t>(peekStateBase + (totalStateSize - nautilus::val<uint64_t>{8}));
                gateHasEntry = nautilus::val<bool>{true};
            }
        }
    }
    if (gateHasEntry and not gateIsKeyframe and not nautilus::invoke(isKeyframeReadyProxy, operatorHandlerMemRef, gateInterval))
    {
        executionCtx.setOpenReturnState(OpenReturnState::REPEAT, KEYFRAME_RETRY_INTERVAL);
        return nautilus::val<uint64_t>{0};
    }

    /// Open the children only AFTER the gate has committed this pass (same ordering as ScanPhysicalOperator::rawScan,
    /// which repeats before openChild). EmitPhysicalOperator::open takes a buffer from the pool via getBufferBlocking,
    /// and a rescheduled pass never reaches the matching close(), so opening above the gate would make every retry
    /// acquire and drop a buffer it never writes to — pure churn on a hot path that, with a drained pool, blocks the
    /// worker on the very query that is already waiting for a keyframe.
    openChild(executionCtx, recordBuffer);

    /// Rare backstop only: clear the final hash map so a post-combine REPEAT (keyframe evicted between the gate
    /// above and the emit below — not reachable while the contiguous probe watermark is held by this un-emitted
    /// window) re-combines cleanly instead of doubling. No-op on the common path (the map starts/stays empty here).
    nautilus::invoke(clearFinalHashMapProxy, aggregationWindowRef);

    /// Combine all keys from all per-thread hash maps into the final hash map (identical to AggregationProbe).
    ChainedHashMapRef finalHashMap(
        finalHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);
    for (nautilus::val<uint64_t> curHashMap = 0; curHashMap < numberOfHashMaps; ++curHashMap)
    {
        const nautilus::val<HashMap*> hashMapPtr = hashMapRefs[curHashMap];
        const ChainedHashMapRef currentMap(
            hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);
        for (const auto entry : currentMap)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRef(entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            finalHashMap.insertOrUpdateEntry(
                entryRef.entryRef,
                [fieldKeys = hashMapOptions.fieldKeys,
                 fieldValues = hashMapOptions.fieldValues,
                 &executionCtx,
                 &entryRef,
                 &aggregationPhysicalFunctions = aggregationPhysicalFunctions,
                 hashMapPtr = hashMapPtr](const nautilus::val<AbstractHashMapEntry*>& entryOnUpdate)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(entryOnUpdate, hashMapPtr, fieldKeys, fieldValues);
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        aggFunction->combine(globalState, entryRefState, executionCtx.pipelineMemoryProvider);
                        globalState = globalState + aggFunction->getSizeOfStateInBytes();
                        entryRefState = entryRefState + aggFunction->getSizeOfStateInBytes();
                    }
                },
                [fieldKeys = hashMapOptions.fieldKeys,
                 fieldValues = hashMapOptions.fieldValues,
                 &executionCtx,
                 &entryRef,
                 &aggregationPhysicalFunctions = aggregationPhysicalFunctions,
                 hashMapPtr = hashMapPtr](const nautilus::val<AbstractHashMapEntry*>& entryOnInsert)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(entryOnInsert, hashMapPtr, fieldKeys, fieldValues);
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        aggFunction->reset(globalState, executionCtx.pipelineMemoryProvider);
                        aggFunction->combine(globalState, entryRefStatePtr, executionCtx.pipelineMemoryProvider);
                        globalState = globalState + aggFunction->getSizeOfStateInBytes();
                        entryRefStatePtr = entryRefStatePtr + aggFunction->getSizeOfStateInBytes();
                    }
                },
                executionCtx.pipelineMemoryProvider.bufferProvider);
        }
    }

    /// Lower each final state WITH the keyframe baseline: keyframe windows lower against zero (emit full) and
    /// publish their interval reference; delta windows lower against their interval's keyframe reference.
    for (const auto entry : finalHashMap)
    {
        const ChainedHashMapRef::ChainedEntryRef entryRef(entry, finalHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
        const auto recordKey = entryRef.getKey();
        const auto stateBase = static_cast<nautilus::val<int8_t*>>(entryRef.getValueMemArea());

        /// This window's keyframe flag and interval id. GEN: computed deterministically from the window ordinal
        /// (once per window, uniform across entries). RESOLVER: the values GEN stamped on the wire, carried in
        /// the resolver state's two trailing words — flag then interval id, right after the histogram region.
        nautilus::val<bool> entryIsKeyframe{false};
        nautilus::val<uint64_t> intervalIndex{0};
        if (isResolver)
        {
            const auto flagWord = readValueFromMemRef<uint64_t>(stateBase + (totalStateSize - nautilus::val<uint64_t>{16}));
            entryIsKeyframe = flagWord != nautilus::val<uint64_t>{0};
            intervalIndex = readValueFromMemRef<uint64_t>(stateBase + (totalStateSize - nautilus::val<uint64_t>{8}));
        }
        else
        {
            entryIsKeyframe = genIsKeyframe;
            intervalIndex = genIntervalIndex;
        }

        /// Baseline for this window: keyframe => shared ZERO baseline (emit the full histogram); delta => the
        /// interval's published keyframe reference. A delta whose keyframe is NOT yet published gets nullptr and
        /// reschedules its whole task (below), emitting nothing this pass. That keeps every task to at most one
        /// emitted record, which is required: several varsized delta-blob emits from one task corrupt the
        /// downstream varsized child buffers.
        nautilus::val<int8_t*> baselineBase{nullptr};
        if (entryIsKeyframe)
        {
            baselineBase = nautilus::invoke(zeroBaselineForProxy, operatorHandlerMemRef, totalStateSize);
        }
        else
        {
            baselineBase = nautilus::invoke(tryGetKeyframeBaselineProxy, operatorHandlerMemRef, intervalIndex, windowEndRaw);
            if (baselineBase == nautilus::val<int8_t*>{nullptr})
            {
                /// Keyframe not published yet: reschedule this task and retry later. Do NOT emit, do NOT reset the
                /// final hash map — the rescheduled pass clears and re-combines it (clearFinalHashMapProxy above).
                executionCtx.setOpenReturnState(OpenReturnState::REPEAT, KEYFRAME_RETRY_INTERVAL);
                return nautilus::val<uint64_t>{0};
            }
        }

        Record outputRecord;
        auto finalStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
        auto baselinePtr = static_cast<nautilus::val<AggregationState*>>(baselineBase);
        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            outputRecord.reassignFields(
                aggFunction->lower(finalStatePtr, baselinePtr, entryIsKeyframe, intervalIndex, executionCtx.pipelineMemoryProvider));
            finalStatePtr = finalStatePtr + aggFunction->getSizeOfStateInBytes();
            baselinePtr = baselinePtr + aggFunction->getSizeOfStateInBytes();
        }

        if (entryIsKeyframe)
        {
            /// Publish this keyframe's state as the interval reference so its deltas can reconstruct against it.
            /// lower against a zero baseline does not mutate stateBase, so it stays this keyframe's histogram.
            nautilus::invoke(publishKeyframeProxy, operatorHandlerMemRef, intervalIndex, stateBase, totalStateSize, windowEndRaw);
        }

        outputRecord.reassignFields(recordKey);
        outputRecord.write(windowMetaData.windowStartFieldName, windowStart.convertToValue());
        outputRecord.write(windowMetaData.windowEndFieldName, windowEnd.convertToValue());
        executeChild(executionCtx, outputRecord);

        for (auto finalStatePtrCleanup = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
             const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            aggFunction->cleanup(finalStatePtrCleanup);
            finalStatePtrCleanup = finalStatePtrCleanup + aggFunction->getSizeOfStateInBytes();
        }
    }

    nautilus::invoke(
        +[](EmittedAggregationWindow* emittedAggregationWindow)
        {
            NES_TRACE(
                "Resetting final hash map of emitted aggregation window start at {} and end at {}",
                emittedAggregationWindow->windowInfo.windowStart,
                emittedAggregationWindow->windowInfo.windowEnd);
            emittedAggregationWindow->finalHashMap.reset();
        },
        aggregationWindowRef);
    return recordBuffer.getNumRecords();
}

DeltaCompressionAggregationProbePhysicalOperator::DeltaCompressionAggregationProbePhysicalOperator(
    HashMapOptions hashMapOptions,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData,
    const uint64_t keyframeInterval,
    const bool isResolver)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , aggregationPhysicalFunctions(std::move(aggregationPhysicalFunctions))
    , hashMapOptions(std::move(hashMapOptions))
    , totalStateSizeInBytes(std::accumulate(
          this->aggregationPhysicalFunctions.begin(),
          this->aggregationPhysicalFunctions.end(),
          static_cast<uint64_t>(0),
          [](uint64_t acc, const std::shared_ptr<AggregationPhysicalFunction>& fn) { return acc + fn->getSizeOfStateInBytes(); }))
    , keyframeInterval(std::max<uint64_t>(1, keyframeInterval))
    , isResolver(isResolver)
{
    /// open() locates the resolver's keyframe flag and interval id at `totalStateSizeInBytes - 16` and `- 8`,
    /// i.e. relative to the SUM of all aggregation state sizes. That only addresses the delta function's own
    /// trailing words while it is the sole aggregation in this probe; with a second synopsis in the same
    /// statistic build the offsets would land in another function's state and be read as the keyframe flag.
    INVARIANT(
        this->aggregationPhysicalFunctions.size() == 1,
        "Delta compression expects exactly one aggregation function, got {}",
        this->aggregationPhysicalFunctions.size());
}
}
