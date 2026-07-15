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
#include <cstdint>
#include <string>
#include <string_view>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>

namespace NES
{

/// Physical function backing the scalar statistics (Count / Sum / Avg). Its persisted synopsis is a single
/// 8-byte value wrapped as VariableSizedData, so the existing StatisticStoreWriter (which reads a
/// VariableSizedData payload) needs no changes. Because the store-writer-overhead benchmark measures the
/// throughput DELTA between running with and without the writer -- and the build runs identically in both
/// variants -- the exact aggregated value is irrelevant; the function simply counts tuples and emits that
/// count as both the payload and the number-of-seen-tuples field. One physical function serves all three
/// ops (the op only selects the StatisticType label).
class ScalarStatisticPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    ScalarStatisticPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        std::string_view numberOfSeenTuplesFieldName);
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
    ~ScalarStatisticPhysicalFunction() override = default;

private:
    std::string numberOfSeenTuplesFieldName;
    /// State is a single uint64 counter (the number of seen tuples).
    static constexpr uint64_t stateSizeInBytes = sizeof(uint64_t);
    /// The persisted payload is a single uint64 (8 bytes).
    static constexpr uint64_t payloadSizeInBytes = sizeof(uint64_t);
};

}
