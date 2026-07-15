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

#include <Aggregation/Function/AvgAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ExecutionContext.hpp>
#include <select.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

AvgAggregationPhysicalFunction::AvgAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const bool includeNullValues)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , includeNullValues(includeNullValues)
{
}

void AvgAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto sum = readSum(aggregationState);
    const auto count = readCount(aggregationState);
    if (inputType.nullable)
    {
        /// If the value is null and we do not include null values, we need to set the multiplication factor to 0
        const auto multiplicationFactor
            = nautilus::select(not includeNullValues and value.isNull(), nautilus::val<int8_t>{0}, nautilus::val<int8_t>{1});

        /// Updating the sum and count with the new value
        const auto newSum = (sum + (value * multiplicationFactor)).castToType(inputType.type);
        const auto newCount = count + multiplicationFactor;

        /// Writing the new isNull, sum, and count back to the aggregation state
        newSum.writeToMemory(sumMemArea(aggregationState));
        newCount.writeToMemory(countMemArea(aggregationState));
        storeNull(aggregationState, newSum.isNull());
    }
    else
    {
        /// Updating the sum and count with the new value
        const auto newSum = (sum + value).castToType(inputType.type);
        const auto newCount = count + nautilus::val<uint64_t>{1};

        /// Writing the new sum, and count back to the aggregation state
        newSum.writeToMemory(sumMemArea(aggregationState));
        newCount.writeToMemory(countMemArea(aggregationState));
    }
}

void AvgAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Combining the sum and count of both aggregation states into the first one
    const auto newSum = (readSum(aggregationState1) + readSum(aggregationState2)).castToType(inputType.type);
    const auto newCount = readCount(aggregationState1) + readCount(aggregationState2);

    newSum.writeToMemory(sumMemArea(aggregationState1));
    newCount.writeToMemory(countMemArea(aggregationState1));
    if (inputType.nullable)
    {
        storeNull(aggregationState1, newSum.isNull());
    }
}

nautilus::val<int8_t*> AvgAggregationPhysicalFunction::sumMemArea(const nautilus::val<AggregationState*>& aggregationState) const
{
    /// The isNull byte, when present, precedes the sum
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    return inputType.nullable ? memArea + nautilus::val<uint64_t>{1} : memArea;
}

nautilus::val<int8_t*> AvgAggregationPhysicalFunction::countMemArea(const nautilus::val<AggregationState*>& aggregationState) const
{
    return sumMemArea(aggregationState) + nautilus::val<uint64_t>(inputType.getSizeInBytesWithoutNull());
}

VarVal AvgAggregationPhysicalFunction::readSum(const nautilus::val<AggregationState*>& aggregationState) const
{
    if (inputType.nullable)
    {
        return VarVal::readVarValFromMemory(sumMemArea(aggregationState), inputType, readNull(aggregationState));
    }
    return VarVal::readNonNullableVarValFromMemory(sumMemArea(aggregationState), inputType);
}

VarVal AvgAggregationPhysicalFunction::readCount(const nautilus::val<AggregationState*>& aggregationState) const
{
    return VarVal::readNonNullableVarValFromMemory(countMemArea(aggregationState), countType);
}

Record AvgAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Calculating the average and returning a record with the result
    const auto sum = readSum(aggregationState);
    const auto count = readCount(aggregationState);
    const auto avg = sum.castToType(resultType.type) / count.castToType(resultType.type);
    return Record({{resultFieldIdentifier, avg}});
}

void AvgAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    /// Resetting the isNull, sum, and count to 0
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    nautilus::memset(memArea, 0, getSizeOfStateInBytes());
}

void AvgAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t AvgAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// Size of isNull + size of the sum value + size of the count value
    const auto inputSize = inputType.getSizeInBytesWithoutNull();
    const auto countTypeSize = countType.getSizeInBytesWithoutNull();
    return (inputType.nullable ? sizeof(bool) : 0) + inputSize + countTypeSize;
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterAvgAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<AvgAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.includeNullValues);
}

}
