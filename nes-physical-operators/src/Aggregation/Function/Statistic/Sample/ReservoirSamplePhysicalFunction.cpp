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
#include <std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>

namespace NES
{
namespace
{
/// @brief Calculates the record size for a sample. The difference is that we need to get the
nautilus::val<uint64_t> getRecordDataSizeForSample(const Record& record, const Schema& schema)
{
    /// For each var sized data, we need to add the size of it
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
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector);

    const auto curRecordIdxPtr = pagedVectorPtr + nautilus::val<uint64_t>(sizeof(Interface::PagedVector));
    const auto sampleDataSizePtr = curRecordIdxPtr + nautilus::val<uint64_t>(sizeof(uint64_t));
    auto currRecordIdx = Nautilus::Util::readValueFromMemRef<uint64_t>(curRecordIdxPtr);
    auto sampleDataSize = Nautilus::Util::readValueFromMemRef<uint64_t>(sampleDataSizePtr);

    if (currRecordIdx < sampleSize)
    {
        pagedVectorRef.writeRecord(record, pipelineMemoryProvider.bufferProvider);
        sampleDataSize = sampleDataSize + getRecordDataSizeForSample(record, memProviderPagedVector->getMemoryLayout()->getSchema());
    }
    else
    {
        /// Replace records in the sample with gradually decreasing probability
        const auto randomNumber = invoke(getRandomNumberProxy, currRecordIdx, nautilus::val<uint64_t>{seed});
        if (randomNumber < sampleSize)
        {
            const auto oldRecord = pagedVectorRef.replaceRecord(record, randomNumber, pipelineMemoryProvider.bufferProvider);
            sampleDataSize = sampleDataSize + getRecordDataSizeForSample(record, memProviderPagedVector->getMemoryLayout()->getSchema())
                - getRecordDataSizeForSample(oldRecord, memProviderPagedVector->getMemoryLayout()->getSchema());
        }
    }
    currRecordIdx = currRecordIdx + nautilus::val<uint64_t>(1);

    VarVal(currRecordIdx).writeToMemory(curRecordIdxPtr);
    VarVal(sampleDataSize).writeToMemory(sampleDataSizePtr);
}

void ReservoirSamplePhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1, nautilus::val<AggregationState*> aggregationState2, PipelineMemoryProvider&)
{
    /// We combine two paged vector by copying all tuples from aggState2 into aggState1
    /// Additionally, we need to sum the sample data size and the cur record idx to keep a

    const auto pagedVectorPtr1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto pagedVectorPtr2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);

    const auto curRecordIdxPtr1 = pagedVectorPtr1 + nautilus::val<uint64_t>(sizeof(Interface::PagedVector));
    const auto sampleDataSizePtr1 = curRecordIdxPtr1 + nautilus::val<uint64_t>(sizeof(uint64_t));
    auto currRecordIdx1 = Nautilus::Util::readValueFromMemRef<uint64_t>(curRecordIdxPtr1);
    auto sampleDataSize1 = Nautilus::Util::readValueFromMemRef<uint64_t>(sampleDataSizePtr1);

    const auto curRecordIdxPtr2 = pagedVectorPtr2 + nautilus::val<uint64_t>(sizeof(Interface::PagedVector));
    const auto sampleDataSizePtr2 = curRecordIdxPtr2 + nautilus::val<uint64_t>(sizeof(uint64_t));
    auto currRecordIdx2 = Nautilus::Util::readValueFromMemRef<uint64_t>(curRecordIdxPtr2);
    auto sampleDataSize2 = Nautilus::Util::readValueFromMemRef<uint64_t>(sampleDataSizePtr2);

    const auto curRecordIdx = currRecordIdx1 + currRecordIdx2;
    VarVal{curRecordIdx}.writeToMemory(curRecordIdxPtr1);
    const auto sampleDataSize = sampleDataSize1 + sampleDataSize2;
    VarVal{sampleDataSize}.writeToMemory(sampleDataSizePtr1);

    invoke(
        +[](Interface::PagedVector* vector1, const Interface::PagedVector* vector2) -> void { vector1->copyFrom(*vector2); },
        pagedVectorPtr1,
        pagedVectorPtr2);
}

Nautilus::Record
ReservoirSamplePhysicalFunction::lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector);
    const auto& schema = memProviderPagedVector->getMemoryLayout()->getSchema();

    const auto curRecordIdxPtr = pagedVectorPtr + nautilus::val<uint64_t>(sizeof(Interface::PagedVector));
    const auto sampleDataSizePtr = curRecordIdxPtr + nautilus::val<uint64_t>(sizeof(uint64_t));
    auto currRecordIdx = Nautilus::Util::readValueFromMemRef<uint64_t>(curRecordIdxPtr);
    auto sampleDataSize = Nautilus::Util::readValueFromMemRef<uint64_t>(sampleDataSizePtr);

    /// Acquiring memory for the sample. We need enough for the sample with its meta data + 4B for storing the size of the variable sized data
    nautilus::val<uint32_t> metaDataSize{8};
    nautilus::val<uint32_t> sizeOfMetaDataSize{32};
    nautilus::val<uint64_t> sizeOfTotalSize{8};
    nautilus::val<uint64_t> totalSize = sampleDataSize + metaDataSize + sizeOfMetaDataSize;
    const auto requiredMemoryInBytes = sampleDataSize + metaDataSize + sizeOfMetaDataSize + sizeOfTotalSize + 4;
    auto sampleMemory = pipelineMemoryProvider.arena.allocateMemory(requiredMemoryInBytes);

    /// Need to offset by 4 bytes as the first four bytes store the size of the variable sized data
    auto currentSampleMemoryPtr = sampleMemory + 4;

    /// Writing the tuples one after the other from the paged vector to the sample memory
    nautilus::val<uint64_t> sampleCounter = 0;
    for (auto sampleIt = pagedVectorRef.begin(schema.getFieldNames()); sampleIt != pagedVectorRef.end(schema.getFieldNames()); ++sampleIt)
    {
        const auto sampleRecord = *sampleIt;
        for (const auto& field : schema)
        {
            const auto& value = sampleRecord.read(field.name);
            if (field.dataType.isSameDataType<VariableSizedData>())
            {
                const auto varSizedValue = value.cast<VariableSizedData>();
                nautilus::memcpy(currentSampleMemoryPtr, varSizedValue.getReference(), varSizedValue.getTotalSize());
                currentSampleMemoryPtr += varSizedValue.getTotalSize();
            }
            else
            {
                /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16
                /// We get the correct function to call via a unordered_map
                if (const auto storeFunction = Nautilus::Util::storeValueFunctionMap.find(field.dataType.type);
                    storeFunction != Nautilus::Util::storeValueFunctionMap.end())
                {
                    auto _ = storeFunction->second(value, currentSampleMemoryPtr);
                    currentSampleMemoryPtr += field.dataType.getSizeInBytes();
                }
                else
                {
                    throw UnknownDataType("Physical Type: {} is currently not supported", field.dataType);
                }
            }
        }
    }

    /// Writing the meta data and the total size
    VarVal{totalSize}.writeToMemory(currentSampleMemoryPtr + 4);
    VarVal{metaDataSize}.writeToMemory(currentSampleMemoryPtr + 4 + sizeOfTotalSize);
    VarVal{sampleCounter}.writeToMemory(currentSampleMemoryPtr + 4 + sizeOfTotalSize + sizeOfMetaDataSize);


    /// Writing the size of the variable size to the first 4 bytes
    nautilus::val<uint32_t> sizeOfVariableSizedData{requiredMemoryInBytes};
    VarVal{sizeOfVariableSizedData}.writeToMemory(sampleMemory);


    /// Add the reservoir to the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, VariableSizedData{sampleMemory});
    return resultRecord;
}

void ReservoirSamplePhysicalFunction::reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVector = reinterpret_cast<Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Interface::PagedVector();

            /// MemSet the two uint64_t values to 0
            std::memset(reinterpret_cast<int8_t*>(pagedVector) + sizeof(Interface::PagedVector), 0, sizeof(uint64_t) * 2);
        },
        aggregationState);
}

void ReservoirSamplePhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector
                = reinterpret_cast<Interface::PagedVector*>(pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}

size_t ReservoirSamplePhysicalFunction::getSizeOfStateInBytes() const
{
    /// PagedVector + curRecordIdx + sampleDataSize (in Bytes)
    return sizeof(Interface::PagedVector) + sizeof(uint64_t) + sizeof(uint64_t);
}

ReservoirSamplePhysicalFunction::ReservoirSamplePhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector,
    const uint64_t seed,
    const uint64_t sampleSize)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , memProviderPagedVector(std::move(memProviderPagedVector))
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
    INVARIANT(arguments.memProviderPagedVector.has_value(), "Memory provider paged vector not set");
    INVARIANT(arguments.seed.has_value(), "Seed not set");
    INVARIANT(arguments.sampleSize.has_value(), "Sample size not set");
    return std::make_shared<ReservoirSamplePhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.memProviderPagedVector.value(),
        arguments.seed.value(),
        arguments.sampleSize.value());
}

}
