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
#include <memory>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

class AvgAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    AvgAggregationPhysicalFunction(
        DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        nautilus::val<TupleBuffer*>,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<TupleBuffer*>,
        nautilus::val<AggregationState*> aggregationState2,
        nautilus::val<TupleBuffer*>,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<TupleBuffer*>,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<TupleBuffer*>,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~AvgAggregationPhysicalFunction() override = default;

    static AggregationPhysicalFunctionRegistryReturnType create(AggregationPhysicalFunctionRegistryArguments arguments);

protected:
    /// The state is [optional isNull byte][sum : resultType][count : countType]. These locate and read the two
    /// halves so that subclasses reusing this state -- the scalar statistics, which persist a sum or an average
    /// as a statistic synopsis -- do not have to repeat the offset math.
    [[nodiscard]] nautilus::val<int8_t*> sumMemArea(const nautilus::val<AggregationState*>& aggregationState) const;
    [[nodiscard]] nautilus::val<int8_t*> countMemArea(const nautilus::val<AggregationState*>& aggregationState) const;

    /// Reads the accumulated sum. Note the isNull byte records "no non-null input has been seen yet", not the
    /// nullness of the sum itself, so the sum is always read as non-null; callers short-circuit to NULL on the
    /// flag, the way lower() does.
    [[nodiscard]] VarVal readSum(const nautilus::val<AggregationState*>& aggregationState) const;
    [[nodiscard]] VarVal readCount(const nautilus::val<AggregationState*>& aggregationState) const;

    DataType countType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
};

}
