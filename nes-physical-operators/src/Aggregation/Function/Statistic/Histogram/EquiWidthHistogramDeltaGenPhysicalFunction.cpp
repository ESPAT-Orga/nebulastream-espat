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

#include <Aggregation/Function/Statistic/Histogram/EquiWidthHistogramDeltaGenPhysicalFunction.hpp>

#include <cstdint>
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
constexpr uint64_t DELTA_HEADER_SIZE = 24; /// 3 * sizeof(uint64_t)
constexpr uint64_t KEYFRAME_FLAG_BIT = uint64_t{1} << 63;
/// Bin-count cut-off below which the per-bin loops unroll with a static_val<> (faster runtime code) rather
/// than a runtime val<> loop. Mirrors the measured value in EquiWidthHistogramPhysicalFunction: above it the
/// unrolled IR makes query compilation explode.
constexpr uint64_t kMaxStaticUnrollBins = 224;
}

Record EquiWidthHistogramDeltaGenPhysicalFunction::lower(
    nautilus::val<AggregationState*> aggregationState,
    nautilus::val<AggregationState*> baselineState,
    const nautilus::val<bool>& isKeyframe,
    const nautilus::val<uint64_t>& intervalId,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// The delta-aware probe operator always passes a valid baseline (a zeroed state for a keyframe window).
    const auto counterSize = dataTypeCounter.getSizeInBytesWithoutNull();
    const auto entrySize = nautilus::val<uint64_t>{sizeof(uint64_t) + counterSize};

    /// Pass 1: count bins whose counter changed.
    nautilus::val<uint64_t> numChanged{0};
    {
        auto curRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{counterOffset};
        auto baseRef = static_cast<nautilus::val<int8_t*>>(baselineState) + nautilus::val<uint64_t>{counterOffset};
        auto countBinIfChanged = [&]
        {
            const auto cur = VarVal::readNonNullableVarValFromMemory(curRef, dataTypeCounter);
            const auto base = VarVal::readNonNullableVarValFromMemory(baseRef, dataTypeCounter);
            if ((cur != base).getRawValueAs<nautilus::val<bool>>())
            {
                numChanged = numChanged + nautilus::val<uint64_t>{1};
            }
            curRef += nautilus::val<uint64_t>{totalBinSize};
            baseRef += nautilus::val<uint64_t>{totalBinSize};
        };
        if (numberOfBins < kMaxStaticUnrollBins)
        {
            for (nautilus::static_val<uint64_t> i = 0; i < numberOfBins; ++i)
            {
                countBinIfChanged();
            }
        }
        else
        {
            for (nautilus::val<uint64_t> i = 0; i < numberOfBins; ++i)
            {
                countBinIfChanged();
            }
        }
    }

    /// nTuplesDelta = current - baseline (mod 2^64).
    const auto nTuplesOffset = nautilus::val<uint64_t>{totalBinSize * numberOfBins};
    const auto curNTuples = readValueFromMemRef<uint64_t>(static_cast<nautilus::val<int8_t*>>(aggregationState) + nTuplesOffset);
    const auto baseNTuples = readValueFromMemRef<uint64_t>(static_cast<nautilus::val<int8_t*>>(baselineState) + nTuplesOffset);
    const auto nTuplesDelta = curNTuples - baseNTuples;

    /// Allocate and write the header. Pack the keyframe flag into bit 63 of the numChangedBins word.
    const auto totalSize = nautilus::val<uint64_t>{DELTA_HEADER_SIZE} + (numChanged * entrySize);
    const auto memArea = pipelineMemoryProvider.arena.allocateMemory(totalSize);
    nautilus::val<uint64_t> numChangedWord = numChanged;
    if (isKeyframe)
    {
        numChangedWord = numChanged | nautilus::val<uint64_t>{KEYFRAME_FLAG_BIT};
    }
    VarVal{numChangedWord}.writeToMemory(memArea);
    VarVal{nTuplesDelta}.writeToMemory(memArea + nautilus::val<uint64_t>{8});
    /// Third header word: this window's keyframe-interval id, for the RESOLVER to group deltas with their keyframe.
    VarVal{intervalId}.writeToMemory(memArea + nautilus::val<uint64_t>{16});

    /// Pass 2: write the changed entries.
    {
        auto entryPtr = memArea + nautilus::val<uint64_t>{DELTA_HEADER_SIZE};
        auto curRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{counterOffset};
        auto baseRef = static_cast<nautilus::val<int8_t*>>(baselineState) + nautilus::val<uint64_t>{counterOffset};
        auto writeBinIfChanged = [&](const nautilus::val<uint64_t>& binIndex)
        {
            const auto cur = VarVal::readNonNullableVarValFromMemory(curRef, dataTypeCounter);
            const auto base = VarVal::readNonNullableVarValFromMemory(baseRef, dataTypeCounter);
            if ((cur != base).getRawValueAs<nautilus::val<bool>>())
            {
                VarVal{binIndex}.writeToMemory(entryPtr);
                const auto delta = cur - base;
                delta.writeToMemory(entryPtr + nautilus::val<uint64_t>{sizeof(uint64_t)});
                entryPtr += entrySize;
            }
            curRef += nautilus::val<uint64_t>{totalBinSize};
            baseRef += nautilus::val<uint64_t>{totalBinSize};
        };
        if (numberOfBins < kMaxStaticUnrollBins)
        {
            for (nautilus::static_val<uint64_t> i = 0; i < numberOfBins; ++i)
            {
                writeBinIfChanged(nautilus::val<uint64_t>{i});
            }
        }
        else
        {
            for (nautilus::val<uint64_t> i = 0; i < numberOfBins; ++i)
            {
                writeBinIfChanged(i);
            }
        }
    }

    Record record;
    record.write(numberOfSeenTuplesFieldName, nTuplesDelta);
    record.write(resultFieldIdentifier, VariableSizedData{memArea, totalSize});
    return record;
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterEquiWidthHistogramDeltaGenAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.numberOfSeenTuplesFieldName.has_value(), "Number of seen tuples is not set");
    INVARIANT(arguments.counterType.has_value(), "counterType is not set");
    INVARIANT(arguments.numberOfBins.has_value(), "Number of buckets is not set");
    INVARIANT(arguments.minValue.has_value(), "Min value is not set");
    INVARIANT(arguments.maxValue.has_value(), "Max value is not set");

    return std::make_shared<EquiWidthHistogramDeltaGenPhysicalFunction>(
        std::move(arguments.inputType),
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
