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
#include <DataTypes/DataType.hpp>

namespace NES
{
class EquiWidthHistogramPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    EquiWidthHistogramPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        const std::string_view numberOfSeenTuplesFieldName,
        uint64_t numberOfBins,
        uint64_t minValue,
        uint64_t maxValue);
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
    ~EquiWidthHistogramPhysicalFunction() override;

private:
    std::string numberOfSeenTuplesFieldName;
    uint64_t numberOfBins;
    uint64_t minValue;
    uint64_t maxValue;
    uint64_t binWidth;
    DataType dataTypeCounter;
    DataType dataTypeLowerUpperBound{DataType::Type::UINT64};
    uint64_t totalBinSize;
    uint64_t counterOffset;
};
}
