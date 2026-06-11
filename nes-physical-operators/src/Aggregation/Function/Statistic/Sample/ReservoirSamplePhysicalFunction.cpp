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

#include <Aggregation/Function/Statistic/Sample/ReservoirSamplePhysicalFunction.hpp>

#include <cstdint>
#include <numeric>
#include <random>
#include <ranges>
#include <thread>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Util.hpp>
#include <Statistic/Sample/ReservoirSampleIteratorImpl.hpp>
#include <std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>

namespace NES
{
namespace
{
/// @brief Calculates the record size for a sample.
nautilus::val<uint64_t> getRecordDataSizeForSample(const Record& record, const TupleBufferRef& buffRef)
{
    auto recordDataSize = nautilus::val<uint64_t>{buffRef.getTupleSize()};
    auto names = buffRef.getAllFieldNames();
    auto types = buffRef.getAllDataTypes();
    for (nautilus::static_val<size_t> i = 0; i < names.size(); ++i)
    {
        auto type = types[i];
        if (type.isSameDataType<VariableSizedData>())
        {
            const auto textValue = record.read(names[i]).getRawValueAs<VariableSizedData>();
            recordDataSize += textValue.getSize() + nautilus::val<uint64_t>{4};
        }
    }
    return recordDataSize;
}

uint64_t getRandomNumberProxy(const uint64_t upperBound, const uint64_t seed)
{
    /// Each thread gets its own RNG, seeded by combining the provided seed
    /// with the thread id to ensure different threads produce different sequences.
    thread_local std::mt19937_64 gen(seed ^ std::hash<std::thread::id>{}(std::this_thread::get_id()));
    std::uniform_int_distribution<uint64_t> dis{0, upperBound};

    return dis(gen);
}

/// @brief Computes a statistically correct plan for merging two reservoirs into a single uniform sample.
///
/// Reservoir 1 is a uniform sample of size m1 = min(k, n1) drawn from n1 elements, reservoir 2 is a
/// uniform sample of size m2 = min(k, n2) drawn from n2 elements. The merged reservoir must be a uniform
/// sample of size targetSize = min(k, n1 + n2) from the union. We achieve this by drawing
/// d1 = the number of survivors taken from reservoir 1 as a hypergeometric variate (sampling targetSize
/// items without replacement from a population of n1 + n2 of which n1 belong to stream 1), then taking a
/// uniform subset of size d1 from reservoir 1 and a uniform subset of size d2 = targetSize - d1 from
/// reservoir 2. Concatenating the two reservoirs (the previous behaviour) is biased and unbounded in size.
///
/// Writes the (m1 - d1) reservoir-1 slot indices to drop into @p dropSlotsOut and the d2 reservoir-2 entry
/// indices to take into @p r2PicksOut (both ascending), and returns d1. The hypergeometric support
/// guarantees d1 <= m1 and d2 <= m2, so the index arrays never overflow the available entries.
uint64_t computeMergePlanProxy(
    const uint64_t n1,
    const uint64_t n2,
    const uint64_t m1,
    const uint64_t m2,
    const uint64_t targetSize,
    const uint64_t seed,
    int8_t* dropSlotsOut,
    int8_t* r2PicksOut)
{
    thread_local std::mt19937_64 gen(seed ^ std::hash<std::thread::id>{}(std::this_thread::get_id()));

    /// Draw d1 ~ Hypergeometric by sampling targetSize items without replacement from the union.
    uint64_t d1 = 0;
    uint64_t remainingSuccess = n1;
    uint64_t remainingTotal = n1 + n2;
    for (uint64_t i = 0; i < targetSize; ++i)
    {
        std::uniform_int_distribution<uint64_t> dist{0, remainingTotal - 1};
        if (dist(gen) < remainingSuccess)
        {
            ++d1;
            --remainingSuccess;
        }
        --remainingTotal;
    }

    /// Knuth's Algorithm S: select `need` distinct ascending indices from [0, population) uniformly.
    /// `gen` has thread-local storage duration, so it is used directly rather than captured.
    const auto selectInto = [](const uint64_t population, const uint64_t need, int8_t* out)
    {
        auto* indices = reinterpret_cast<uint64_t*>(out);
        uint64_t remaining = population;
        uint64_t needLeft = need;
        uint64_t written = 0;
        for (uint64_t pos = 0; pos < population && needLeft > 0; ++pos)
        {
            std::uniform_int_distribution<uint64_t> dist{0, remaining - 1};
            if (dist(gen) < needLeft)
            {
                indices[written] = pos;
                ++written;
                --needLeft;
            }
            --remaining;
        }
    };

    selectInto(m1, m1 - d1, dropSlotsOut);
    selectInto(m2, targetSize - d1, r2PicksOut);
    return d1;
}
}

void ReservoirSamplePhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);

    const auto numberOfSeenTuplesRef = pagedVectorPtr + nautilus::val<uint64_t>{sizeof(PagedVector)};
    const auto sampleDataSizeRef = numberOfSeenTuplesRef + nautilus::val<uint64_t>{sizeof(uint64_t)};
    auto numberOfSeenTuples = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);
    auto sampleDataSize = readValueFromMemRef<uint64_t>(sampleDataSizeRef);

    if (numberOfSeenTuples < sampleSize)
    {
        pagedVectorRef.writeRecord(record, pipelineMemoryProvider.bufferProvider);
        sampleDataSize = sampleDataSize + getRecordDataSizeForSample(record, *bufferRef);
    }
    else
    {
        /// Replace records in the sample with gradually decreasing probability
        const auto randomNumber = invoke(getRandomNumberProxy, numberOfSeenTuples, nautilus::val<uint64_t>{seed});
        if (randomNumber < sampleSize)
        {
            const auto oldRecord = pagedVectorRef.replaceRecord(record, randomNumber, pipelineMemoryProvider.bufferProvider);
            sampleDataSize
                = sampleDataSize + getRecordDataSizeForSample(record, *bufferRef) - getRecordDataSizeForSample(oldRecord, *bufferRef);
        }
    }
    numberOfSeenTuples = numberOfSeenTuples + nautilus::val<uint64_t>{1};

    VarVal{numberOfSeenTuples}.writeToMemory(numberOfSeenTuplesRef);
    VarVal{sampleDataSize}.writeToMemory(sampleDataSizeRef);
}

void ReservoirSamplePhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Merge two partial reservoirs into a single uniform sample of size min(sampleSize, n1 + n2).
    /// Simply concatenating the reservoirs is statistically wrong:
    /// It biases the sample whenever n1 != n2 and grows the result beyond sampleSize. Instead we keep a
    /// hypergeometric number of survivors from reservoir 1 and fill the rest from reservoir 2.
    const auto pagedVectorPtr1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto pagedVectorPtr2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    const PagedVectorRef pagedVectorRef1(pagedVectorPtr1, bufferRef);
    const PagedVectorRef pagedVectorRef2(pagedVectorPtr2, bufferRef);

    const auto numberOfSeenTuplesRef1 = pagedVectorPtr1 + nautilus::val<uint64_t>{sizeof(PagedVector)};
    const auto sampleDataSizeRef1 = numberOfSeenTuplesRef1 + nautilus::val<uint64_t>{sizeof(uint64_t)};
    const auto numberOfSeenTuplesRef2 = pagedVectorPtr2 + nautilus::val<uint64_t>{sizeof(PagedVector)};

    const auto numberOfSeenTuples1 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef1);
    const auto numberOfSeenTuples2 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef2);
    auto sampleDataSize = readValueFromMemRef<uint64_t>(sampleDataSizeRef1);

    /// Number of entries physically stored in each reservoir: m1 = min(sampleSize, n1), m2 = min(sampleSize, n2).
    const auto numberStored1 = pagedVectorRef1.getNumberOfTuples();
    const auto numberStored2 = pagedVectorRef2.getNumberOfTuples();

    /// Target sample size after the merge: min(sampleSize, n1 + n2). Note that targetSize >= numberStored1
    /// always holds, so we never have to shrink reservoir 1 below its current size.
    const auto totalSeen = numberOfSeenTuples1 + numberOfSeenTuples2;
    auto targetSize = totalSeen;
    if (targetSize > nautilus::val<uint64_t>{sampleSize})
    {
        targetSize = nautilus::val<uint64_t>{sampleSize};
    }

    /// Scratch memory for the merge plan: up to numberStored1 drop-slot indices followed by up to
    /// targetSize reservoir-2 pick indices, each a uint64_t.
    const auto indexSize = nautilus::val<uint64_t>{sizeof(uint64_t)};
    const auto planMemory = pipelineMemoryProvider.arena.allocateMemory((numberStored1 + targetSize) * indexSize);
    const auto dropSlotsRef = planMemory;
    const auto r2PicksRef = planMemory + numberStored1 * indexSize;

    const auto survivorsFrom1 = invoke(
        computeMergePlanProxy,
        numberOfSeenTuples1,
        numberOfSeenTuples2,
        numberStored1,
        numberStored2,
        targetSize,
        nautilus::val<uint64_t>{seed},
        dropSlotsRef,
        r2PicksRef);
    const auto dropCount = numberStored1 - survivorsFrom1;
    const auto totalFromReservoir2 = targetSize - survivorsFrom1;

    const auto fieldNames = bufferRef->getAllFieldNames();

    /// Overwrite the dropped reservoir-1 slots with the first reservoir-2 survivors.
    for (nautilus::val<uint64_t> i = 0; i < dropCount; ++i)
    {
        const auto slot = readValueFromMemRef<uint64_t>(dropSlotsRef + i * indexSize);
        const auto pick = readValueFromMemRef<uint64_t>(r2PicksRef + i * indexSize);
        const auto newRecord = pagedVectorRef2.readRecord(pick, fieldNames);
        const auto oldRecord = pagedVectorRef1.replaceRecord(newRecord, slot, pipelineMemoryProvider.bufferProvider);
        sampleDataSize
            = sampleDataSize + getRecordDataSizeForSample(newRecord, *bufferRef) - getRecordDataSizeForSample(oldRecord, *bufferRef);
    }

    /// Append the remaining reservoir-2 survivors.
    for (nautilus::val<uint64_t> i = dropCount; i < totalFromReservoir2; ++i)
    {
        const auto pick = readValueFromMemRef<uint64_t>(r2PicksRef + i * indexSize);
        const auto newRecord = pagedVectorRef2.readRecord(pick, fieldNames);
        pagedVectorRef1.writeRecord(newRecord, pipelineMemoryProvider.bufferProvider);
        sampleDataSize = sampleDataSize + getRecordDataSizeForSample(newRecord, *bufferRef);
    }

    VarVal{totalSeen}.writeToMemory(numberOfSeenTuplesRef1);
    VarVal{sampleDataSize}.writeToMemory(sampleDataSizeRef1);
}

Record
ReservoirSamplePhysicalFunction::lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);

    const auto numberOfSeenTuplesRef = pagedVectorPtr + nautilus::val<uint64_t>{sizeof(PagedVector)};
    const auto sampleDataSizeRef = numberOfSeenTuplesRef + nautilus::val<uint64_t>{sizeof(uint64_t)};
    auto numberOfSeenTuples = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);
    auto sampleDataSize = readValueFromMemRef<uint64_t>(sampleDataSizeRef);

    /// Acquiring memory for the sample. We need enough for the sample with its meta data  for storing the size of the
    /// variable sized data that doubles as the total size of the synopses
    ReservoirSampleHeaderRef header;
    const auto requiredMemoryInBytes = header.getTotalSize(sampleDataSize);
    auto sampleMemory = pipelineMemoryProvider.arena.allocateMemory(requiredMemoryInBytes);
    nautilus::memset(sampleMemory, 0, requiredMemoryInBytes);
    header.setMemArea(sampleMemory);

    /// Writing the tuples one after the other from the paged vector to the sample memory
    nautilus::val<uint64_t> tuplesInSample = 0;
    auto sampleTuplesMemArea = header.getSampleMemArea();
    const auto fieldNames = bufferRef->getAllFieldNames();
    for (auto sampleIt = pagedVectorRef.begin(fieldNames); sampleIt != pagedVectorRef.end(fieldNames); ++sampleIt)
    {
        const auto sampleRecord = *sampleIt;
        const auto names = bufferRef->getAllFieldNames();
        const auto types = bufferRef->getAllDataTypes();
        for (nautilus::static_val<size_t> i = 0; i < names.size(); ++i)
        {
            const auto name = names[i];
            const auto type = types[i];
            const auto& value = sampleRecord.read(name);
            /// As we store varsized data directly in the sample area, we need to handle it ourselves.
            if (type.isSameDataType<VariableSizedData>())
            {
                const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
                const auto contentSize = varSizedValue.getSize();
                /// Write the size prefix as uint32_t followed by the content
                VarVal{static_cast<nautilus::val<uint32_t>>(contentSize)}.writeToMemory(sampleTuplesMemArea);
                nautilus::memcpy(sampleTuplesMemArea + nautilus::val<uint64_t>{4}, varSizedValue.getContent(), contentSize);
                sampleTuplesMemArea += contentSize + nautilus::val<uint64_t>{4};
            }
            else
            {
                /// For all other data types, we can reuse the existing store value function map
                if (const auto storeFunction = storeValueFunctionMap.find(type.type); storeFunction != storeValueFunctionMap.end())
                {
                    auto _ = storeFunction->second(value, sampleTuplesMemArea);
                    sampleTuplesMemArea += type.getSizeInBytesWithoutNull();
                }
                else
                {
                    throw UnknownDataType("Physical Type: {} is currently not supported", type);
                }
            }
        }
        tuplesInSample += 1;
    }

    /// Writing the meta data and the total size
    header.setSampleSize(tuplesInSample);
    header.writeMetaData();

    /// Add the reservoir to the result record
    Record resultRecord;
    resultRecord.write(numberOfSeenTuplesFieldName, numberOfSeenTuples);
    resultRecord.write(resultFieldIdentifier, VariableSizedData{sampleMemory, requiredMemoryInBytes});
    return resultRecord;
}

void ReservoirSamplePhysicalFunction::reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea, const uint64_t stateSize) -> void
        {
            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea);
            new (pagedVector) PagedVector();

            /// Zero the trailing fields after the PagedVector (numberOfSeenTuples and sampleDataSize).
            /// Writing `stateSize` bytes here would overflow past the entry's state area and corrupt the next entry.
            std::memset(reinterpret_cast<int8_t*>(pagedVector) + sizeof(PagedVector), 0, stateSize - sizeof(PagedVector));
        },
        aggregationState,
        nautilus::val<uint64_t>{getSizeOfStateInBytes()});
}

void ReservoirSamplePhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}

size_t ReservoirSamplePhysicalFunction::getSizeOfStateInBytes() const
{
    /// PagedVector + numberOfSeenTuples + sampleDataSize (in Bytes)
    return sizeof(PagedVector) + sizeof(uint64_t) + sizeof(uint64_t);
}

ReservoirSamplePhysicalFunction::ReservoirSamplePhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<TupleBufferRef> bufferRef,
    const std::string_view numberOfSeenTuplesFieldName,
    const uint64_t seed,
    const uint64_t sampleSize)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , numberOfSeenTuplesFieldName(numberOfSeenTuplesFieldName)
    , seed(seed)
    , sampleSize(sampleSize)
{
}

ReservoirSamplePhysicalFunction::~ReservoirSamplePhysicalFunction()
{
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterReservoirSampleAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.numberOfSeenTuplesFieldName.has_value(), "Number of seen tuples is not set");
    INVARIANT(arguments.seed.has_value(), "Seed is not set");
    INVARIANT(arguments.sampleSize.has_value(), "Sample size is not set");
    return std::make_shared<ReservoirSamplePhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.bufferRefPagedVector.value(),
        arguments.numberOfSeenTuplesFieldName.value(),
        arguments.seed.value(),
        arguments.sampleSize.value());
}

}
