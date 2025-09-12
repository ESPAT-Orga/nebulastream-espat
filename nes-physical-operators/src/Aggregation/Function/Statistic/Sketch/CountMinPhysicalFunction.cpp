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

#include <Aggregation/Function/Statistic/Sketch/CountMinPhysicalFunction.hpp>

#include <algorithm>
#include <random>
#include <ranges>
#include <Nautilus/Interface/Hash/H3HashFunction.hpp>
#include <std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>

namespace NES
{
CountMinPhysicalFunction::CountMinPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    const std::string_view numberOfSeenTuplesFieldName,
    const uint64_t numberOfCols,
    const uint64_t numberOfRows,
    const uint64_t seed)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , numberOfSeenTuplesFieldName(numberOfSeenTuplesFieldName)
    , counterType(resultType)
    , numberOfCols(numberOfCols)
    , numberOfRows(numberOfRows)
    , totalSizeOfSketchInBytes(numberOfRows * numberOfCols * counterType.getSizeInBytes())
    , numberOfBitsInKey(sizeof(Nautilus::Interface::HashFunction::HashValue::raw_type) * 8)
    , sizeOfSingleSeed(sizeof(Nautilus::Interface::HashFunction::HashValue::raw_type) * 8)
    , sizeOfSeedsForOneCol(numberOfBitsInKey * sizeOfSingleSeed)
    , seed(seed)
{
}

void CountMinPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto firstCounterRef = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto firstSeedsRef = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{totalSizeOfSketchInBytes};
    for (nautilus::static_val<uint64_t> col = 0; col < numberOfCols; ++col)
    {
        const auto seedsRef = firstSeedsRef + nautilus::val<uint64_t>{col * sizeOfSeedsForOneCol};
        Interface::H3HashFunction h3HashFunction{sizeOfSingleSeed, numberOfBitsInKey, seedsRef};
        auto hash = h3HashFunction.HashFunction::calculate(value);
        auto row = hash % numberOfRows;
        auto counterRef = firstCounterRef + counterType.getSizeInBytes() * (col * numberOfRows + row);
        auto counter = VarVal::readVarValFromMemory(counterRef, counterType.type);
        counter = counter + nautilus::val<uint64_t>{1};
        counter.writeToMemory(counterRef);
    }

    /// Incrementing the number of seen tuples
    const auto numberOfSeenTuplesRef = static_cast<nautilus::val<int8_t*>>(aggregationState)
        + nautilus::val<uint64_t>{totalSizeOfSketchInBytes + sizeOfSeedsForOneCol * numberOfCols};
    auto numberOfSeenTuples = Nautilus::Util::readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);
    numberOfSeenTuples = numberOfSeenTuples + 1;
    VarVal{numberOfSeenTuples}.writeToMemory(numberOfSeenTuplesRef);
}

void CountMinPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1, nautilus::val<AggregationState*> aggregationState2, PipelineMemoryProvider&)
{
    /// We assume that both count min sketches have the same counter type and no. rows / cols.
    /// Therefore, we can combine the counters by adding them together
    auto counterLeftRef = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    auto counterRightRef = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    for (nautilus::static_val<uint64_t> counterIdx = 0; counterIdx < numberOfCols * numberOfRows; ++counterIdx)
    {
        auto counterLeft = VarVal::readVarValFromMemory(counterLeftRef, counterType.type);
        auto counterRight = VarVal::readVarValFromMemory(counterRightRef, counterType.type);
        counterLeft = counterLeft + counterRight;
        counterLeft.writeToMemory(counterLeftRef);

        counterLeftRef += counterType.getSizeInBytes();
        counterRightRef += counterType.getSizeInBytes();
    }

    /// Combining the number of seen tuples of both histograms
    const auto numberOfSeenTuplesRef1 = static_cast<nautilus::val<int8_t*>>(aggregationState1)
        + nautilus::val<uint64_t>{totalSizeOfSketchInBytes + sizeOfSeedsForOneCol * numberOfCols};
    auto numberOfSeenTuples1 = Nautilus::Util::readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef1);
    const auto numberOfSeenTuplesRef2 = static_cast<nautilus::val<int8_t*>>(aggregationState2)
        + nautilus::val<uint64_t>{totalSizeOfSketchInBytes + sizeOfSeedsForOneCol * numberOfCols};
    auto numberOfSeenTuples2 = Nautilus::Util::readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef2);
    numberOfSeenTuples1 = numberOfSeenTuples1 + numberOfSeenTuples2;
    VarVal{numberOfSeenTuples1}.writeToMemory(numberOfSeenTuplesRef1);
}

Nautilus::Record
CountMinPhysicalFunction::lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Need to acquire new memory, as the current allocated aggregation state for the counters will be deleted.
    /// We need an additional 4B for the size of the variable sized data, as we store the statistics as var-sized data.
    const auto countMinSize = totalSizeOfSketchInBytes + 4;
    const auto countMinMemory = pipelineMemoryProvider.arena.allocateMemory(countMinSize);

    /// Copying all counters to the newly acquired memory and adding the size of the variable sized data (count min sketch)
    nautilus::memcpy(countMinMemory + nautilus::val<uint64_t>{4}, aggregationState, totalSizeOfSketchInBytes);
    const nautilus::val<uint32_t> sizeOfVariableSizedData{static_cast<uint32_t>(totalSizeOfSketchInBytes)};
    VarVal{sizeOfVariableSizedData}.writeToMemory(countMinMemory);

    /// Reading the number of seen tuples
    const auto numberOfSeenTuplesRef = static_cast<nautilus::val<int8_t*>>(aggregationState)
        + nautilus::val<uint64_t>{totalSizeOfSketchInBytes + sizeOfSeedsForOneCol * numberOfCols};
    auto numberOfSeenTuples = Nautilus::Util::readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);

    /// Adding the count min to the result record
    Record record;
    record.write(numberOfSeenTuplesFieldName, numberOfSeenTuples);
    record.write(resultFieldIdentifier, VariableSizedData{countMinMemory});
    return record;
}

void CountMinPhysicalFunction::reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting all counters and the number of seen tuples to 0
    nautilus::memset(aggregationState, 0, totalSizeOfSketchInBytes + sizeof(uint64_t));

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
        nautilus::val<uint64_t>{sizeOfSeedsForOneCol * numberOfCols},
        nautilus::val<uint64_t>{seed});
}

void CountMinPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t CountMinPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Last u64 is for storing the number of seen tuples
    return counterType.getSizeInBytes() * numberOfCols * numberOfRows + sizeOfSeedsForOneCol * numberOfCols + sizeof(uint64_t);
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterCountMinSketchAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.numberOfSeenTuplesFieldName.has_value(), "Number of seen tuples is not set");
    INVARIANT(arguments.columns.has_value(), "Columns value is not set");
    INVARIANT(arguments.rows.has_value(), "Rows value is not set");
    INVARIANT(arguments.seed.has_value(), "Seed value is not set");

    return std::make_shared<CountMinPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.numberOfSeenTuplesFieldName.value(),
        arguments.columns.value(),
        arguments.rows.value(),
        arguments.seed.value());
}
}
