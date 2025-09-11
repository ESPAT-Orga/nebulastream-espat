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

#include <Aggregation/Function/Statistic/Histogram/EquiWidthHistogramPhysicalFunction.hpp>


#include <cstdint>
#include <numeric>
#include <random>
#include <ranges>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <std/cstring.h>

#include <AggregationPhysicalFunctionRegistry.hpp>

namespace NES
{
void EquiWidthHistogramPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    /// Getting the bin index
    auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    auto binIdx = (value.cast<nautilus::val<uint64_t>>() - nautilus::val<uint64_t>{minValue}) / nautilus::val<uint64_t>{binWidth};
    if (binIdx >= numberOfBins)
    {
        /// If the value is maxValue, we would get a bin index that is larger than the number of bins
        binIdx = numberOfBins - 1;
    }

    /// Calculating the memory ref for the counter and incrementing it
    auto counterRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{counterOffset};
    counterRef = counterRef + nautilus::val<uint64_t>{binIdx * totalBinSize};
    auto counter = VarVal::readVarValFromMemory(counterRef, dataTypeCounter.type);
    counter = counter + nautilus::val<uint8_t>{1};
    counter.writeToMemory(counterRef);

    /// Incrementing the number of seen tuples
    const auto numberOfSeenTuplesRef
        = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{totalBinSize * numberOfBins};
    auto numberOfSeenTuples = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);
    numberOfSeenTuples = numberOfSeenTuples + 1;
    VarVal{numberOfSeenTuples}.writeToMemory(numberOfSeenTuplesRef);
}

void EquiWidthHistogramPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1, nautilus::val<AggregationState*> aggregationState2, PipelineMemoryProvider&)
{
    /// We assume that both histograms have the exact same min, max, and number of bins.
    /// Therefore, we can simply iterate through all bins and then sum up their counters
    auto leftCounterRef = static_cast<nautilus::val<int8_t*>>(aggregationState1) + nautilus::val<uint64_t>{counterOffset};
    auto rightCounterRef = static_cast<nautilus::val<int8_t*>>(aggregationState2) + nautilus::val<uint64_t>{counterOffset};

    for (nautilus::static_val<uint64_t> counterIdx = 0; counterIdx < numberOfBins; ++counterIdx)
    {
        auto leftCounter = VarVal::readVarValFromMemory(leftCounterRef, dataTypeCounter.type);
        auto rightCounter = VarVal::readVarValFromMemory(rightCounterRef, dataTypeCounter.type);
        rightCounter = rightCounter + leftCounter;
        rightCounter.writeToMemory(leftCounterRef);

        /// Incrementing left and right counter ref
        leftCounterRef += totalBinSize;
        rightCounterRef += totalBinSize;
    }

    /// Combining the number of seen tuples of both histograms
    const auto numberOfSeenTuplesRef1
        = static_cast<nautilus::val<int8_t*>>(aggregationState1) + nautilus::val<uint64_t>{totalBinSize * numberOfBins};
    auto numberOfSeenTuples1 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef1);
    const auto numberOfSeenTuplesRef2
        = static_cast<nautilus::val<int8_t*>>(aggregationState2) + nautilus::val<uint64_t>{totalBinSize * numberOfBins};
    auto numberOfSeenTuples2 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef2);
    numberOfSeenTuples1 = numberOfSeenTuples1 + numberOfSeenTuples2;
    VarVal{numberOfSeenTuples1}.writeToMemory(numberOfSeenTuplesRef1);
}

Record
EquiWidthHistogramPhysicalFunction::lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Need to acquire new memory, as the current memory for the bins will be deleted.
    /// We need an additional 4B for the size of the variable sized data, as we store the statistics as var-sized data.
    const auto histogramMemorySize = getSizeOfStateInBytes() + 4;
    const auto histogramMemory = pipelineMemoryProvider.arena.allocateMemory(histogramMemorySize);

    /// Copying all bins to the newly acquired memory and adding the size of the variable sized data (histogram)
    nautilus::memcpy(histogramMemory + nautilus::val<uint64_t>{4}, aggregationState, getSizeOfStateInBytes());
    const nautilus::val<uint32_t> sizeOfVariableSizedData{static_cast<uint32_t>(getSizeOfStateInBytes())};
    VarVal{sizeOfVariableSizedData}.writeToMemory(histogramMemory);

    /// Reading the number of seen tuples
    const auto numberOfSeenTuplesRef
        = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{totalBinSize * numberOfBins};
    auto numberOfSeenTuples = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);

    /// Adding the histogram to the result record
    Record record;
    record.write(numberOfSeenTuplesFieldName, numberOfSeenTuples);
    record.write(resultFieldIdentifier, VariableSizedData{histogramMemory});
    return record;
}

void EquiWidthHistogramPhysicalFunction::reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Going through all buckets, setting their lower, upper bound and resetting their counter to 0
    auto lowerBoundRef = static_cast<nautilus::val<int8_t*>>(aggregationState);
    auto counterRef = lowerBoundRef + nautilus::val<uint64_t>{counterOffset};
    auto upperBoundRef = counterRef + nautilus::val<uint64_t>{counterOffset + dataTypeCounter.getSizeInBytes()};
    VarVal lowerBound{minValue};
    VarVal upperBound{minValue + binWidth};
    auto counter = VarVal::readVarValFromMemory(counterRef, dataTypeCounter.type);
    counter = counter - counter; /// Getting a 0 regardless of the counter data type

    for (nautilus::static_val<uint64_t> counterIdx = 0; counterIdx < numberOfBins; ++counterIdx)
    {
        /// Writing the current values to the current locations and then incrementing the lower and upper bound
        lowerBound.writeToMemory(lowerBoundRef);
        upperBound.writeToMemory(upperBoundRef);
        counter.writeToMemory(counterRef);
        lowerBound = lowerBound + nautilus::val<uint64_t>{binWidth};
        upperBound = upperBound + nautilus::val<uint64_t>{binWidth};

        /// Incrementing the refs
        lowerBoundRef += nautilus::val<uint64_t>{totalBinSize};
        upperBoundRef += nautilus::val<uint64_t>{totalBinSize};
        counterRef += nautilus::val<uint64_t>{totalBinSize};
    }

    /// Resetting the seen number of tuples
    const auto numberOfSeenTuplesRef
        = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{totalBinSize * numberOfBins};
    VarVal{nautilus::val<uint64_t>{0}}.writeToMemory(numberOfSeenTuplesRef);
}

void EquiWidthHistogramPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t EquiWidthHistogramPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Size of the histogram with each bin containing a counter and lower/upper bound. Also we need 64bit for the numberOfSeenTuples
    return numberOfBins * (dataTypeCounter.getSizeInBytes() + 2 * dataTypeLowerUpperBound.getSizeInBytes()) + sizeof(uint64_t);
}

EquiWidthHistogramPhysicalFunction::EquiWidthHistogramPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const std::string_view numberOfSeenTuplesFieldName,
    const uint64_t numberOfBins,
    const uint64_t minValue,
    const uint64_t maxValue)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , numberOfSeenTuplesFieldName(numberOfSeenTuplesFieldName)
    , numberOfBins(numberOfBins)
    , minValue(minValue)
    , maxValue(maxValue)
    , binWidth((maxValue - minValue) / numberOfBins)
    , dataTypeCounter(resultType)
    , totalBinSize((dataTypeCounter.getSizeInBytes() + 2 * dataTypeLowerUpperBound.getSizeInBytes()))
    , counterOffset(dataTypeLowerUpperBound.getSizeInBytes())
{
}

EquiWidthHistogramPhysicalFunction::~EquiWidthHistogramPhysicalFunction()
{
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterEquiWidthHistogramAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.numberOfSeenTuplesFieldName.has_value(), "Number of seen tuples is not set");
    INVARIANT(arguments.numberOfBins.has_value(), "Number of buckets is not set");
    INVARIANT(arguments.minValue.has_value(), "Min value is not set");
    INVARIANT(arguments.maxValue.has_value(), "Max value is not set");

    return std::make_shared<EquiWidthHistogramPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.numberOfSeenTuplesFieldName.value(),
        arguments.numberOfBins.value(),
        arguments.minValue.value(),
        arguments.maxValue.value());
}

}
