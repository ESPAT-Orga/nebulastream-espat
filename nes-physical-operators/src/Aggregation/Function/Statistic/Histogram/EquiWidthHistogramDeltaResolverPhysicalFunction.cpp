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

#include <Aggregation/Function/Statistic/Histogram/EquiWidthHistogramDeltaResolverPhysicalFunction.hpp>

#include <cstdint>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>

namespace NES
{

/// Blob layout: see the class comment in EquiWidthHistogramDeltaGenPhysicalFunction.hpp.
namespace
{
constexpr uint64_t DELTA_HEADER_SIZE = 24;
constexpr uint64_t KEYFRAME_FLAG_BIT = uint64_t{1} << 63;
constexpr uint64_t NUM_CHANGED_MASK = ~KEYFRAME_FLAG_BIT;
/// Bin-count cut-off below which the per-bin loop unrolls with a static_val<> (faster runtime code) rather
/// than a runtime val<> loop. Mirrors the measured value in EquiWidthHistogramPhysicalFunction: above it the
/// unrolled IR makes query compilation explode.
constexpr uint64_t kMaxStaticUnrollBins = 224;
}

void EquiWidthHistogramDeltaResolverPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    /// The "input" of the resolver is the incoming sparse-delta blob (inputFunction = field access to it).
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto blob = value.getRawValueAs<VariableSizedData>();
    const auto content = blob.getContent();

    const auto numChangedWord = readValueFromMemRef<uint64_t>(content);
    const auto numChanged = numChangedWord & nautilus::val<uint64_t>{NUM_CHANGED_MASK};
    const auto nTuplesDelta = readValueFromMemRef<uint64_t>(content + nautilus::val<uint64_t>{8});

    /// Record this window's keyframe flag (bit 63) into the state's trailing flag WORD, for the probe to read.
    const auto keyframeFlag = numChangedWord & nautilus::val<uint64_t>{KEYFRAME_FLAG_BIT};
    const auto flagRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{keyframeFlagOffset()};
    VarVal{keyframeFlag}.writeToMemory(flagRef);

    /// Record this window's keyframe-interval id (blob header word 2) into the state's trailing id WORD, for
    /// the probe to read as the keyframe-cache key.
    const auto intervalId = readValueFromMemRef<uint64_t>(content + nautilus::val<uint64_t>{16});
    const auto intervalIdRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{intervalIdOffset()};
    VarVal{intervalId}.writeToMemory(intervalIdRef);

    /// nTuples += nTuplesDelta
    const auto nTuplesRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{totalBinSize * numberOfBins};
    const auto newNTuples = readValueFromMemRef<uint64_t>(nTuplesRef) + nTuplesDelta;
    VarVal{newNTuples}.writeToMemory(nTuplesRef);

    /// Apply each changed bin's counter delta.
    const auto entrySize = nautilus::val<uint64_t>{sizeof(uint64_t) + dataTypeCounter.getSizeInBytesWithoutNull()};
    auto entryPtr = content + nautilus::val<uint64_t>{DELTA_HEADER_SIZE};
    for (nautilus::val<uint64_t> e = 0; e < numChanged; e = e + nautilus::val<uint64_t>{1})
    {
        const auto binIndex = readValueFromMemRef<uint64_t>(entryPtr);
        const auto counterDelta
            = VarVal::readNonNullableVarValFromMemory(entryPtr + nautilus::val<uint64_t>{sizeof(uint64_t)}, dataTypeCounter);
        const auto counterRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{counterOffset}
            + (binIndex * nautilus::val<uint64_t>{totalBinSize});
        const auto cur = VarVal::readNonNullableVarValFromMemory(counterRef, dataTypeCounter);
        (cur + counterDelta).writeToMemory(counterRef);
        entryPtr += entrySize;
    }
}

Record EquiWidthHistogramDeltaResolverPhysicalFunction::lower(
    nautilus::val<AggregationState*> aggregationState,
    nautilus::val<AggregationState*> baselineState,
    const nautilus::val<bool>& /*isKeyframe*/,
    const nautilus::val<uint64_t>& /*intervalId*/,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Add the baseline onto the state -> full histogram. Bounds are unchanged.
    auto curRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{counterOffset};
    auto baseRef = static_cast<nautilus::val<int8_t*>>(baselineState) + nautilus::val<uint64_t>{counterOffset};
    auto addBaselineOntoBin = [&]
    {
        const auto cur = VarVal::readNonNullableVarValFromMemory(curRef, dataTypeCounter);
        const auto base = VarVal::readNonNullableVarValFromMemory(baseRef, dataTypeCounter);
        (cur + base).writeToMemory(curRef);
        curRef += nautilus::val<uint64_t>{totalBinSize};
        baseRef += nautilus::val<uint64_t>{totalBinSize};
    };
    if (numberOfBins < kMaxStaticUnrollBins)
    {
        for (nautilus::static_val<uint64_t> i = 0; i < numberOfBins; ++i)
        {
            addBaselineOntoBin();
        }
    }
    else
    {
        for (nautilus::val<uint64_t> i = 0; i < numberOfBins; ++i)
        {
            addBaselineOntoBin();
        }
    }

    const auto nTuplesOffset = nautilus::val<uint64_t>{totalBinSize * numberOfBins};
    const auto nRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nTuplesOffset;
    const auto nbRef = static_cast<nautilus::val<int8_t*>>(baselineState) + nTuplesOffset;
    VarVal{readValueFromMemRef<uint64_t>(nRef) + readValueFromMemRef<uint64_t>(nbRef)}.writeToMemory(nRef);

    /// Serialise the now-full histogram with the standard blob layout (uses histogramStateBytes(), so the
    /// trailing bookkeeping words are not part of the stored blob).
    return EquiWidthHistogramPhysicalFunction::lower(aggregationState, pipelineMemoryProvider);
}

size_t EquiWidthHistogramDeltaResolverPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Histogram state + two trailing 8-byte words: this window's keyframe flag and interval id (see the header).
    return histogramStateBytes() + (2 * sizeof(uint64_t));
}

void EquiWidthHistogramDeltaResolverPhysicalFunction::reset(
    nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    EquiWidthHistogramPhysicalFunction::reset(aggregationState, pipelineMemoryProvider);
    /// Zero the keyframe flag and interval id words (the base reset only touches the histogram region).
    const auto flagRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{keyframeFlagOffset()};
    VarVal{nautilus::val<uint64_t>{0}}.writeToMemory(flagRef);
    const auto intervalIdRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{intervalIdOffset()};
    VarVal{nautilus::val<uint64_t>{0}}.writeToMemory(intervalIdRef);
}

void EquiWidthHistogramDeltaResolverPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    EquiWidthHistogramPhysicalFunction::combine(aggregationState1, aggregationState2, pipelineMemoryProvider);
    /// OR the keyframe flag words so the flag survives the probe's per-thread-hashmap merge (re-window is 1:1,
    /// so at most one side carries the flag; OR is the safe merge either way).
    const auto flagRef1 = static_cast<nautilus::val<int8_t*>>(aggregationState1) + nautilus::val<uint64_t>{keyframeFlagOffset()};
    const auto flagRef2 = static_cast<nautilus::val<int8_t*>>(aggregationState2) + nautilus::val<uint64_t>{keyframeFlagOffset()};
    const auto merged = readValueFromMemRef<uint64_t>(flagRef1) | readValueFromMemRef<uint64_t>(flagRef2);
    VarVal{merged}.writeToMemory(flagRef1);

    /// Take the max interval id so it survives the merge too. A reset state carries 0; a lifted one carries the
    /// real id; re-window is 1:1 so both lifted sides carry the same id — max keeps the real id in every case.
    const auto idRef1 = static_cast<nautilus::val<int8_t*>>(aggregationState1) + nautilus::val<uint64_t>{intervalIdOffset()};
    const auto idRef2 = static_cast<nautilus::val<int8_t*>>(aggregationState2) + nautilus::val<uint64_t>{intervalIdOffset()};
    const auto id1 = readValueFromMemRef<uint64_t>(idRef1);
    const auto id2 = readValueFromMemRef<uint64_t>(idRef2);
    nautilus::val<uint64_t> mergedId = id1;
    if (id2 > id1)
    {
        mergedId = id2;
    }
    VarVal{mergedId}.writeToMemory(idRef1);
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterEquiWidthHistogramDeltaResolverAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.numberOfSeenTuplesFieldName.has_value(), "Number of seen tuples is not set");
    INVARIANT(arguments.counterType.has_value(), "counterType is not set");
    INVARIANT(arguments.numberOfBins.has_value(), "Number of buckets is not set");
    INVARIANT(arguments.minValue.has_value(), "Min value is not set");
    INVARIANT(arguments.maxValue.has_value(), "Max value is not set");

    /// The base EquiWidthHistogramPhysicalFunction derives the bin-bound byte layout (counterOffset,
    /// totalBinSize) from its `inputType`. For a normal histogram the input IS the counted value, so that
    /// is the value/bound type. The RESOLVER instead aggregates the VARSIZED delta blob (read via
    /// `inputFunction`), so its logical inputType is VARSIZED (16 bytes) — which would size the bounds
    /// wrongly and produce a blob the probe iterator reads at the wrong stride (garbled bins). The bins are
    /// always written as 8-byte uint64 bounds, so pass UINT64 as the layout/bound type here; the actual
    /// VARSIZED blob is still consumed through `inputFunction`, which is independent of this type.
    auto boundType = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    return std::make_shared<EquiWidthHistogramDeltaResolverPhysicalFunction>(
        std::move(boundType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.numberOfSeenTuplesFieldName.value(),
        arguments.counterType.value(),
        arguments.numberOfBins.value(),
        arguments.minValue.value(),
        arguments.maxValue.value());
}

}
