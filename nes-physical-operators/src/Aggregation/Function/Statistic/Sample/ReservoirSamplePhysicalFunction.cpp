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
nautilus::val<uint64_t> getRecordDataSizeForSample(const Record& record, const Schema& schema)
{
    /// For each var sized data, we need to add the size of it. We take the total size of the var sized into account.
    auto recordDataSize = nautilus::val<uint64_t>(schema.getSizeOfSchemaInBytes());
    for (const auto& field : nautilus::static_iterable(schema))
    {
        if (field.dataType.isSameDataType<VariableSizedData>())
        {
            const auto textValue = record.read(field.name).cast<VariableSizedData>();
            recordDataSize += textValue.getTotalSize();
        }
    }

    return recordDataSize;
}

uint64_t getRandomNumberProxy(const uint64_t upperBound, const uint64_t seed)
{
    static std::mt19937 gen(seed);
    std::uniform_int_distribution<> dis(0, upperBound);

    return dis(gen);
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
        sampleDataSize = sampleDataSize + getRecordDataSizeForSample(record, bufferRef->getMemoryLayout()->getSchema());
    }
    else
    {
        /// Replace records in the sample with gradually decreasing probability
        const auto randomNumber = invoke(getRandomNumberProxy, numberOfSeenTuples, nautilus::val<uint64_t>{seed});
        if (randomNumber < sampleSize)
        {
            const auto oldRecord = pagedVectorRef.replaceRecord(record, randomNumber, pipelineMemoryProvider.bufferProvider);
            sampleDataSize = sampleDataSize + getRecordDataSizeForSample(record, bufferRef->getMemoryLayout()->getSchema())
                - getRecordDataSizeForSample(oldRecord, bufferRef->getMemoryLayout()->getSchema());
        }
    }
    numberOfSeenTuples = numberOfSeenTuples + nautilus::val<uint64_t>(1);

    VarVal{numberOfSeenTuples}.writeToMemory(numberOfSeenTuplesRef);
    VarVal{sampleDataSize}.writeToMemory(sampleDataSizeRef);
}

void ReservoirSamplePhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1, nautilus::val<AggregationState*> aggregationState2, PipelineMemoryProvider&)
{
    /// We combine two paged vector by copying all tuples from aggState2 into aggState1
    /// Additionally, we need to sum the sample data size and the number of seen tuples

    const auto pagedVectorPtr1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto pagedVectorPtr2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);

    const auto numberOfSeenTuplesRef1 = pagedVectorPtr1 + nautilus::val<uint64_t>{sizeof(PagedVector)};
    const auto sampleDataSizeRef1 = numberOfSeenTuplesRef1 + nautilus::val<uint64_t>{sizeof(uint64_t)};
    auto numberOfSeenTuples1 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef1);
    auto sampleDataSize1 = readValueFromMemRef<uint64_t>(sampleDataSizeRef1);

    const auto numberOfSeenTuplesRef2 = pagedVectorPtr2 + nautilus::val<uint64_t>{sizeof(PagedVector)};
    const auto sampleDataSizeRef2 = numberOfSeenTuplesRef2 + nautilus::val<uint64_t>{sizeof(uint64_t)};
    auto numberOfSeenTuples2 = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef2);
    auto sampleDataSize2 = readValueFromMemRef<uint64_t>(sampleDataSizeRef2);

    const auto numberOfSeenTuples = numberOfSeenTuples1 + numberOfSeenTuples2;
    const auto sampleDataSize = sampleDataSize1 + sampleDataSize2;
    VarVal{numberOfSeenTuples}.writeToMemory(numberOfSeenTuplesRef1);
    VarVal{sampleDataSize}.writeToMemory(sampleDataSizeRef1);

    invoke(
        +[](PagedVector* vector1, const PagedVector* vector2) -> void { vector1->copyFrom(*vector2); }, pagedVectorPtr1, pagedVectorPtr2);
}

Record
ReservoirSamplePhysicalFunction::lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    const auto& schema = bufferRef->getMemoryLayout()->getSchema();

    const auto numberOfSeenTuplesRef = pagedVectorPtr + nautilus::val<uint64_t>{sizeof(PagedVector)};
    const auto sampleDataSizeRef = numberOfSeenTuplesRef + nautilus::val<uint64_t>{sizeof(uint64_t)};
    auto numberOfSeenTuples = readValueFromMemRef<uint64_t>(numberOfSeenTuplesRef);
    auto sampleDataSize = readValueFromMemRef<uint64_t>(sampleDataSizeRef);

    /// Acquiring memory for the sample. We need enough for the sample with its meta data + 4B for storing the size of the
    /// variable sized data that doubles as the total size of the synopses
    ReservoirSampleHeaderRef header;
    const auto requiredMemoryInBytes = header.getTotalSize(sampleDataSize);
    auto sampleMemory = pipelineMemoryProvider.arena.allocateMemory(requiredMemoryInBytes);
    nautilus::memset(sampleMemory, 0, requiredMemoryInBytes);
    header.setMemArea(sampleMemory);

    /// Writing the tuples one after the other from the paged vector to the sample memory
    nautilus::val<uint64_t> tuplesInSample = 0;
    auto sampleTuplesMemArea = header.getSampleMemArea();
    const auto fieldNames = schema.getFieldNames();
    for (auto sampleIt = pagedVectorRef.begin(fieldNames); sampleIt != pagedVectorRef.end(fieldNames); ++sampleIt)
    {
        const auto sampleRecord = *sampleIt;
        for (nautilus::static_val<size_t> i = 0; i < schema.getNumberOfFields(); ++i)
        {
            const auto& field = schema.getFieldAt(i);
            const auto& value = sampleRecord.read(field.name);
            /// As we store varsized data directly in the sample area, we need to handle it ourselves.
            if (field.dataType.isSameDataType<VariableSizedData>())
            {
                const auto varSizedValue = value.cast<VariableSizedData>();
                nautilus::memcpy(sampleTuplesMemArea, varSizedValue.getReference(), varSizedValue.getTotalSize());
                sampleTuplesMemArea += varSizedValue.getTotalSize();
            }
            else
            {
                /// For all other data types, we can reuse the existing store value function map
                if (const auto storeFunction = storeValueFunctionMap.find(field.dataType.type);
                    storeFunction != storeValueFunctionMap.end())
                {
                    auto _ = storeFunction->second(value, sampleTuplesMemArea);
                    sampleTuplesMemArea += field.dataType.getSizeInBytes();
                }
                else
                {
                    throw UnknownDataType("Physical Type: {} is currently not supported", field.dataType);
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
    resultRecord.write(resultFieldIdentifier, VariableSizedData{sampleMemory});
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

            /// MemSet the three uint64_t values to 0
            std::memset(reinterpret_cast<int8_t*>(pagedVector) + sizeof(PagedVector), 0, stateSize);
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
