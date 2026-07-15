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

#include <Aggregation/Function/Statistic/Scalar/ScalarStatisticPhysicalFunction.hpp>

#include <cstdint>
#include <utility>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

ScalarStatisticPhysicalFunction::ScalarStatisticPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const std::string_view numberOfSeenTuplesFieldName)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , numberOfSeenTuplesFieldName(numberOfSeenTuplesFieldName)
{
}

void ScalarStatisticPhysicalFunction::lift(const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider&, const Record&)
{
    /// Count-only: we ignore the input value (see class comment). The build cost cancels in the
    /// with/without-writer throughput delta the benchmark measures.
    const auto counterRef = static_cast<nautilus::val<int8_t*>>(aggregationState);
    auto count = readValueFromMemRef<uint64_t>(counterRef);
    count = count + nautilus::val<uint64_t>{1};
    VarVal{count}.writeToMemory(counterRef);
}

void ScalarStatisticPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1, nautilus::val<AggregationState*> aggregationState2, PipelineMemoryProvider&)
{
    const auto counterRef1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    const auto counterRef2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);
    auto count1 = readValueFromMemRef<uint64_t>(counterRef1);
    const auto count2 = readValueFromMemRef<uint64_t>(counterRef2);
    count1 = count1 + count2;
    VarVal{count1}.writeToMemory(counterRef1);
}

Record
ScalarStatisticPhysicalFunction::lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto counterRef = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto count = readValueFromMemRef<uint64_t>(counterRef);

    /// Persist the 8-byte count as the statistic payload (wrapped as VariableSizedData so the existing
    /// StatisticStoreWriter path handles it unchanged).
    const nautilus::val<uint64_t> payloadSize{payloadSizeInBytes};
    const auto payloadMemory = pipelineMemoryProvider.arena.allocateMemory(payloadSize);
    VarVal{count}.writeToMemory(payloadMemory);

    Record record;
    record.write(numberOfSeenTuplesFieldName, count);
    record.write(resultFieldIdentifier, VariableSizedData{payloadMemory, payloadSize});
    return record;
}

void ScalarStatisticPhysicalFunction::reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::memset(aggregationState, 0, stateSizeInBytes);
}

void ScalarStatisticPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t ScalarStatisticPhysicalFunction::getSizeOfStateInBytes() const
{
    return stateSizeInBytes;
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterScalarStatisticAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.numberOfSeenTuplesFieldName.has_value(), "Number of seen tuples is not set");
    return std::make_shared<ScalarStatisticPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.numberOfSeenTuplesFieldName.value());
}

}
