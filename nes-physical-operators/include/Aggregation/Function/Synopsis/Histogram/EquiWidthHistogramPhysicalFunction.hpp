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

#pragma once

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Synopsis/Histogram/HistogramPhysicalFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>

namespace NES
{

/// The equi width histogram keeps count of the occurrence of values in specified buckets.
/// minValue and maxValue specify the range of values that are to be counted and the bucket width
/// is calculated with numBuckets.
class EquiWidthHistogramPhysicalFunction final : public HistogramPhysicalFunction
{
public:
    EquiWidthHistogramPhysicalFunction(
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
        uint64_t pageSize);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~EquiWidthHistogramPhysicalFunction() override = default;

private:
    nautilus::val<uint64_t> numBuckets;
    nautilus::val<int64_t> minValue;
    nautilus::val<int64_t> maxValue;
    nautilus::val<double_t> bucketWidth;

    std::unique_ptr<Interface::HashFunction> hashFunction;
    Schema bucketSchema;
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldKeys;
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldValues;
    nautilus::val<uint64_t> entriesPerPage;
    nautilus::val<uint64_t> entrySize;
    nautilus::val<uint64_t> pageSize;

    nautilus::val<uint64_t> numFilledBuckets;
};

}
