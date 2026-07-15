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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_concepts.hpp>

namespace NES
{

class AvgAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    AvgAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        bool includeNullValues);
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
    ~AvgAggregationPhysicalFunction() override = default;

protected:
    /// The state is [optional isNull byte][sum : inputType][count : countType]. These locate and read the two halves,
    /// so that subclasses reusing this state (see ScalarStatisticPhysicalFunction.cpp) do not repeat the offset math.
    [[nodiscard]] nautilus::val<int8_t*> sumMemArea(const nautilus::val<AggregationState*>& aggregationState) const;
    [[nodiscard]] nautilus::val<int8_t*> countMemArea(const nautilus::val<AggregationState*>& aggregationState) const;
    [[nodiscard]] VarVal readSum(const nautilus::val<AggregationState*>& aggregationState) const;
    [[nodiscard]] VarVal readCount(const nautilus::val<AggregationState*>& aggregationState) const;

    DataType countType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};

private:
    bool includeNullValues;
};

}
