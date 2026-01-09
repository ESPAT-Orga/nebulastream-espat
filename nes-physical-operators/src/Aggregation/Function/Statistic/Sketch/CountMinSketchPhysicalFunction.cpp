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

#include <Aggregation/Function/Statistic/Sketch/CountMinSketchPhysicalFunction.hpp>

#include <algorithm>
#include <random>
#include <ranges>

#include <Nautilus/Interface/Hash/H3HashFunction.hpp>
#include <std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <Statistic/StatisticProvider.hpp>

namespace NES
{
CountMinSketchPhysicalFunction::CountMinSketchPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const std::string_view numberOfSeenTuplesFieldName,
    DataType counterType,
    const uint64_t numberOfCols,
    const uint64_t numberOfRows,
    const uint64_t seed)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , numberOfSeenTuplesFieldName(numberOfSeenTuplesFieldName)
    , counterType(counterType)
    , numberOfCols(numberOfCols)
    , numberOfRows(numberOfRows)
    , totalSizeOfSketchInBytes(numberOfRows * numberOfCols * counterType.getSizeInBytes())
    , numberOfBitsInKey(sizeof(HashFunction::HashValue::raw_type) * 8)
    , sizeOfSingleSeed(sizeof(HashFunction::HashValue::raw_type))
    , totalSizeOfSeeds(numberOfBitsInKey * sizeOfSingleSeed * numberOfRows)
    , seed(seed)
{
}

void CountMinSketchPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto firstCounterRef = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto firstSeedsRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{totalSizeOfSketchInBytes};
    for (nautilus::static_val<uint64_t> col = 0; col < numberOfCols; ++col)
    {
        const auto seedsRef = firstSeedsRef + nautilus::val<uint64_t>{col * sizeOfSingleSeed};
        H3HashFunction h3HashFunction{sizeOfSingleSeed, numberOfBitsInKey, seedsRef};
        auto hash = h3HashFunction.HashFunction::calculate(value);
        auto row = hash % numberOfRows;
        auto counterRef = firstCounterRef + counterType.getSizeInBytes() * (col * numberOfRows + row);
        auto counter = VarVal::readVarValFromMemory(counterRef, counterType.type);
        counter = counter + nautilus::val<uint64_t>{1};
        counter.writeToMemory(counterRef);
    }

    /// Incrementing the number of seen tuples
    const auto numberOfSeenTuplesRef
        = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{totalSizeOfSketchInBytes + totalSizeOfSeeds};
    auto numberOfSeenTuples = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);
    numberOfSeenTuples = numberOfSeenTuples + 1;
    VarVal{numberOfSeenTuples}.writeToMemory(numberOfSeenTuplesRef);
}

void CountMinSketchPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1, nautilus::val<AggregationState*> aggregationState2, PipelineMemoryProvider&)
{
    /// We assume that both count min sketches have the same counter type, no. rows / cols and seed.
    /// Therefore, we can combine the counters by adding them together
    auto counter1RefTemp = aggregationState1;
    auto counter2RefTemp = aggregationState2;
    auto counter1Ref = static_cast<nautilus::val<int8_t*>>(counter1RefTemp);
    auto counter2Ref = static_cast<nautilus::val<int8_t*>>(counter2RefTemp);
    for (nautilus::static_val<uint64_t> counterIdx = 0; counterIdx < numberOfCols * numberOfRows; ++counterIdx)
    {
        auto counter1 = VarVal::readVarValFromMemory(counter1Ref, counterType.type);
        auto counter2 = VarVal::readVarValFromMemory(counter2Ref, counterType.type);
        counter1 = counter1 + counter2;
        counter1.writeToMemory(counter1Ref);

        counter1Ref += counterType.getSizeInBytes();
        counter2Ref += counterType.getSizeInBytes();
    }
    const auto numberOfSeenTuplesRef1 = static_cast<nautilus::val<int8_t*>>(aggregationState1)
        + nautilus::val<uint64_t>{totalSizeOfSketchInBytes} + nautilus::val<uint64_t>{totalSizeOfSeeds};
    auto numberOfSeenTuples1 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef1);
    const auto numberOfSeenTuplesRef2 = static_cast<nautilus::val<int8_t*>>(aggregationState2)
        + nautilus::val<uint64_t>{totalSizeOfSketchInBytes} + nautilus::val<uint64_t>{totalSizeOfSeeds};
    auto numberOfSeenTuples2 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef2);
    numberOfSeenTuples1 = numberOfSeenTuples1 + numberOfSeenTuples2;
    VarVal{numberOfSeenTuples1}.writeToMemory(numberOfSeenTuplesRef1);
}

Record CountMinSketchPhysicalFunction::lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Need to acquire new memory, as the current allocated aggregation state for the counters will be deleted.
    const auto loweredHeaderSize = StatisticProviderIteratorImpl::sizeOfMetaDataSize + StatisticProviderIteratorImpl::sizeOfTotalAreaSize;
    const nautilus::val<uint32_t> countMinSize = totalSizeOfSketchInBytes + loweredHeaderSize + loweredMetaDataSize;
    const auto countMinMemory = pipelineMemoryProvider.arena.allocateMemory(countMinSize);

    const nautilus::val<uint32_t> sizeOfVariableSizedData{countMinSize};
    VarVal{sizeOfVariableSizedData}.writeToMemory(countMinMemory);
    VarVal{loweredMetaDataSize}.writeToMemory(countMinMemory + nautilus::val<uint64_t>{StatisticProviderIteratorImpl::sizeOfTotalAreaSize});
    VarVal{numberOfRows}.writeToMemory(countMinMemory + nautilus::val<uint64_t>{loweredHeaderSize});
    VarVal{numberOfCols}.writeToMemory(countMinMemory + nautilus::val<uint64_t>{loweredHeaderSize} + nautilus::val<uint64_t>{sizeof(numberOfRows)});
    /// Copying all counters to the newly acquired memory and adding the size of the variable sized data (count min sketch)
    const auto dataOffset = nautilus::val<uint64_t>{loweredHeaderSize + loweredMetaDataSize};
    nautilus::memcpy(countMinMemory + dataOffset, aggregationState, totalSizeOfSketchInBytes);

    /// Reading the number of seen tuples
    const auto numberOfSeenTuplesRef
        = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{totalSizeOfSketchInBytes + totalSizeOfSeeds};
    auto numberOfSeenTuples = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);

    /// Adding the count min to the result record
    Record record;
    record.write(numberOfSeenTuplesFieldName, numberOfSeenTuples);
    record.write(resultFieldIdentifier, VariableSizedData{countMinMemory});
    return record;
}

void CountMinSketchPhysicalFunction::reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting all counters, seeds and the number of seen tuples to 0
    nautilus::memset(aggregationState, 0, totalSizeOfSketchInBytes + totalSizeOfSeeds + sizeof(uint64_t));

    /// Creating seeds for the number of columns
    nautilus::invoke(
        +[](int8_t* seedsMemArea, const uint64_t numberOfRandomBytes, const uint64_t seedVal)
        {
            std::mt19937 gen(seedVal);
            std::uniform_int_distribution<> dis(0, 255);

            /// Generating random bytes starting at seedsMemArea
            auto random_byte = [&dis, &gen]() { return static_cast<unsigned char>(dis(gen)); };
            auto bytes = std::views::iota(static_cast<uint64_t>(0), numberOfRandomBytes)
                | std::views::transform([&](auto) { return random_byte(); });
            std::ranges::copy(bytes, seedsMemArea);
        },
        static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{totalSizeOfSketchInBytes},
        nautilus::val<uint64_t>{totalSizeOfSeeds},
        nautilus::val<uint64_t>{seed});
}

void CountMinSketchPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t CountMinSketchPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Last u64 is for storing the number of seen tuples
    return totalSizeOfSketchInBytes + totalSizeOfSeeds + sizeof(uint64_t);
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterCountMinSketchAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.numberOfSeenTuplesFieldName.has_value(), "Number of seen tuples is not set");
    INVARIANT(arguments.counterType.has_value(), "Counter type is not set");
    INVARIANT(arguments.columns.has_value(), "Columns value is not set");
    INVARIANT(arguments.rows.has_value(), "Rows value is not set");
    INVARIANT(arguments.seed.has_value(), "Seed value is not set");

    return std::make_shared<CountMinSketchPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.numberOfSeenTuplesFieldName.value(),
        arguments.counterType.value(),
        arguments.columns.value(),
        arguments.rows.value(),
        arguments.seed.value());
}
}
