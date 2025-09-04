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

#include <Aggregation/Function/Synopsis/Histogram/EquiWidthHistogramPhysicalFunction.hpp>

#include <cstdint>

#include <Aggregation/Function/Synopsis/SynopsisFunctionRef.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Util/Common.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

EquiWidthHistogramPhysicalFunction::EquiWidthHistogramPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    uint64_t numBuckets,
    int64_t minValue,
    int64_t maxValue,
    std::unique_ptr<Interface::HashFunction> hashFunction,
    const Schema& bucketSchema,
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldKeys,
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldValues,
    uint64_t entriesPerPage,
    uint64_t entrySize,
    uint64_t pageSize)
    : HistogramPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , numBuckets(numBuckets)
    , minValue(minValue)
    , maxValue(maxValue)
    , hashFunction(std::move(hashFunction))
    , bucketSchema(std::move(bucketSchema))
    , fieldKeys(std::move(fieldKeys))
    , fieldValues(std::move(fieldValues))
    , entriesPerPage(entriesPerPage)
    , entrySize(entrySize)
    , pageSize(pageSize)
{
    bucketWidth = static_cast<double_t>(maxValue - minValue) / numBuckets;
}

void EquiWidthHistogramPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto fieldVarVal = inputFunction.execute(record, pipelineMemoryProvider.arena);
    auto bucketIndexDouble = fieldVarVal / bucketWidth;
    /// round the calculated index by casting to uint
    auto bucketIndex = VarVal(bucketIndexDouble.cast<nautilus::val<uint64_t>>());

    Interface::ChainedHashMapRef hashMap(aggregationState, fieldKeys, fieldValues, entriesPerPage, entrySize);

    Record indexRecord;
    indexRecord.write(resultFieldIdentifier, bucketIndex);

    const auto hashMapEntry = hashMap.findOrCreateEntry(
        indexRecord,
        *hashFunction,
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(entry);
            const VarVal zeroVal(nautilus::val<uint64_t>(0));
            zeroVal.writeToMemory(memAreaCount);
            ++numFilledBuckets;
        },
        pipelineMemoryProvider.bufferProvider);

    const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(hashMapEntry);
    const auto count = VarVal(Nautilus::Util::readValueFromMemRef<uint64_t>(memAreaCount) + 1);
    count.writeToMemory(memAreaCount);
}

void EquiWidthHistogramPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    Interface::ChainedHashMapRef hashMap1(aggregationState1, fieldKeys, fieldValues, entriesPerPage, entrySize);
    Interface::ChainedHashMapRef hashMap2(aggregationState2, fieldKeys, fieldValues, entriesPerPage, entrySize);

    /// Iterate hashMap2 and add each bucket count to hashMap1
    for (auto entryIt = hashMap2.begin(); entryIt != hashMap2.end(); ++entryIt)
    {
        const auto entry = *entryIt;
        const Interface::ChainedHashMapRef::ChainedEntryRef entryRefRef(entry, aggregationState2, fieldKeys, fieldValues);
        const auto keyRecord = entryRefRef.getKey();
        const auto valueRecord = entryRefRef.getValue();

        const auto hashMap1Entry = hashMap1.findOrCreateEntry(
            keyRecord,
            *hashFunction,
            [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
            {
                const auto memAreaCount = static_cast<nautilus::val<int8_t*>>(entry);
                const VarVal zeroVal(nautilus::val<uint64_t>(0));
                zeroVal.writeToMemory(memAreaCount);
                ++numFilledBuckets;
            },
            pipelineMemoryProvider.bufferProvider);

        /// assuming there is only one field in the value record
        const auto otherCount = valueRecord.read(fieldValues[0].fieldIdentifier);
        const auto memAreaCount1 = entryRefRef.getValueMemArea();
        const auto countSum = VarVal(Nautilus::Util::readValueFromMemRef<uint64_t>(memAreaCount1)) + otherCount;
        countSum.writeToMemory(memAreaCount1);
    }
}

Record EquiWidthHistogramPhysicalFunction::lower(
    [[maybe_unused]] const nautilus::val<AggregationState*> aggregationState,
    [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider)
{
    Interface::ChainedHashMapRef hashMap(aggregationState, fieldKeys, fieldValues, entriesPerPage, entrySize);

    /// Create a Ref object to store the bucketWidth as metaData and a record with the lower bound and count for each bucket
    const auto dataSize = nautilus::val<uint32_t>(numFilledBuckets * (sizeof(double_t) + sizeof(uint64_t)));
    auto histogramFunctionRef = SynopsisFunctionRef(bucketSchema);
    histogramFunctionRef.initializeForWriting(
        pipelineMemoryProvider.arena, dataSize, nautilus::val<uint32_t>(sizeof(double_t)), VarVal(numFilledBuckets));

    /// Iterate hashmap to fill histogramFunctionRef with all buckets that have a count > 0
    for (auto entryIt = hashMap.begin(); entryIt != hashMap.end(); ++entryIt)
    {
        const auto entryRef = *entryIt;
        const Interface::ChainedHashMapRef::ChainedEntryRef entryRefRef(entryRef, aggregationState, fieldKeys, fieldValues);
        const auto keyRecord = entryRefRef.getKey();
        const auto valueRecord = entryRefRef.getValue();

        Record bucketRecord;
        bucketRecord.reassignFields(keyRecord);
        bucketRecord.reassignFields(valueRecord);
        histogramFunctionRef.writeRecord(bucketRecord);
    }

    /// Add the reservoir to the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, histogramFunctionRef.getSynopsis());
    return resultRecord;
}

void EquiWidthHistogramPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    invoke(
        +[](AggregationState* hashMapMemArea, const uint64_t numBuckets, const uint64_t pageSize) -> void
        {
            /// Allocate a new chained hash map in the memory area provided by the aggregationState pointer
            auto* hashMapPtr = reinterpret_cast<Interface::ChainedHashMap*>(hashMapMemArea);
            new (hashMapPtr) Interface::ChainedHashMap(sizeof(uint64_t), sizeof(uint64_t), numBuckets, pageSize);
        },
        aggregationState,
        numBuckets,
        pageSize);
}

void EquiWidthHistogramPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* hashMapMemArea) -> void
        {
            /// Calls the destructor of the HashMap
            auto* hashMapPtr
                = reinterpret_cast<Interface::ChainedHashMap*>(hashMapMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            hashMapPtr->~ChainedHashMap();
        },
        aggregationState);
}

size_t EquiWidthHistogramPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Interface::ChainedHashMap);
}

/*EquiWidthHistogramPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    uint64_t numBuckets,
    int64_t minValue,
    int64_t maxValue,
    std::unique_ptr<Interface::HashFunction> hashFunction,
    const Schema& bucketSchema,
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldKeys,
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldValues,
    uint64_t entriesPerPage,
    uint64_t entrySize,
    uint64_t pageSize)*/

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterEquiWidthHistogramAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    auto numBuckets = arguments.optionalSynopsisArgs[0];
    auto minValue = arguments.optionalSynopsisArgs[1];
    auto maxValue = arguments.optionalSynopsisArgs[2];
    std::unique_ptr<Interface::HashFunction> hashFunction = std::make_unique<Interface::MurMur3HashFunction>();
    auto bucketSchema = Schema();
    std::vector<Record::RecordFieldIdentifier> fieldValueKeys;
    std::vector<Record::RecordFieldIdentifier> fieldValueNames;
    for (size_t i = 0; i < numBuckets; ++i)
    {
        auto keyName = fmt::format("bucket_{}_lower", i);
        bucketSchema.addField(keyName, DataType::Type::UINT64);
        fieldValueKeys.emplace_back(keyName);
        auto valueName = fmt::format("bucket_{}_count", i);
        bucketSchema.addField(valueName, DataType::Type::UINT64);
        fieldValueNames.emplace_back(valueName);
    }
    auto [fieldKeys, fieldValues]
        = Interface::MemoryProvider::ChainedEntryMemoryProvider::createFieldOffsets(bucketSchema, fieldValueKeys, fieldValueNames);
    /// TODO Are these values set correctly?
    auto keySize = sizeof(uint64_t); /// Size of Record, but that is an unordered_map?
    auto valueSize = sizeof(uint64_t); /// Same as above
    auto entrySize = sizeof(Interface::ChainedHashMapEntry) + keySize + valueSize;
    auto pageSize = DEFAULT_PAGED_VECTOR_SIZE;
    auto entriesPerPage = pageSize / entrySize;

    return std::make_shared<EquiWidthHistogramPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        numBuckets,
        minValue,
        maxValue,
        std::move(hashFunction),
        bucketSchema,
        fieldKeys,
        fieldValues,
        entriesPerPage,
        entrySize,
        pageSize);
}

}
