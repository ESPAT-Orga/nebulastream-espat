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
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>

namespace NES
{
class ReservoirSamplePhysicalFunction final : public AggregationPhysicalFunction
{
public:
    ReservoirSamplePhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef,
        const std::string_view numberOfSeenTuplesFieldName,
        const uint64_t seed,
        const uint64_t sampleSize);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Nautilus::Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Nautilus::Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~ReservoirSamplePhysicalFunction() override;

private:
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef;
    std::string numberOfSeenTuplesFieldName;
    uint64_t seed;
    uint64_t sampleSize;
};
}
