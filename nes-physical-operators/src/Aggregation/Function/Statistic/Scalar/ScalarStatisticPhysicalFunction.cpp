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
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <magic_enum/magic_enum.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

namespace
{
/// Builds the record contract every statistic physical function emits (cf. CountMinSketchPhysicalFunction::lower):
/// exactly two fields -- the number of seen tuples, and the payload as arena-backed VariableSizedData. A scalar
/// statistic carries no metadata, so the payload is the bare value with no header (see ScalarStatisticIteratorImpl).
Record makeScalarStatisticRecord(
    const Record::RecordFieldIdentifier& resultFieldIdentifier,
    const std::string& numberOfSeenTuplesFieldName,
    const VarVal& payload,
    const DataType& payloadType,
    const VarVal& numberOfSeenTuples,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> payloadSize{payloadType.getSizeInBytesWithoutNull()};
    const auto payloadMemory = pipelineMemoryProvider.arena.allocateMemory(payloadSize);
    payload.castToType(payloadType.type).writeToMemory(payloadMemory);

    Record record;
    record.write(numberOfSeenTuplesFieldName, numberOfSeenTuples.castToType(DataType::Type::UINT64));
    record.write(resultFieldIdentifier, VariableSizedData{payloadMemory, payloadSize});
    return record;
}
}

CountStatisticPhysicalFunction::CountStatisticPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const bool includeNullValues,
    const std::string_view numberOfSeenTuplesFieldName)
    : CountAggregationPhysicalFunction(
          std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier), includeNullValues)
    , numberOfSeenTuplesFieldName(numberOfSeenTuplesFieldName)
{
}

Record CountStatisticPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// The counter is the whole state, so we let the base read it and only re-wrap it as the statistic contract.
    const auto count = CountAggregationPhysicalFunction::lower(aggregationState, pipelineMemoryProvider).read(resultFieldIdentifier);
    return makeScalarStatisticRecord(resultFieldIdentifier, numberOfSeenTuplesFieldName, count, resultType, count, pipelineMemoryProvider);
}

SumStatisticPhysicalFunction::SumStatisticPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const bool includeNullValues,
    const std::string_view numberOfSeenTuplesFieldName)
    : AvgAggregationPhysicalFunction(
          std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier), includeNullValues)
    , numberOfSeenTuplesFieldName(numberOfSeenTuplesFieldName)
{
}

Record
SumStatisticPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Unlike AvgAggregationPhysicalFunction::lower we emit the running sum verbatim rather than dividing it by the
    /// count; the count becomes the number of seen tuples instead.
    return makeScalarStatisticRecord(
        resultFieldIdentifier,
        numberOfSeenTuplesFieldName,
        readSum(aggregationState),
        resultType,
        readCount(aggregationState),
        pipelineMemoryProvider);
}

AvgStatisticPhysicalFunction::AvgStatisticPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const bool includeNullValues,
    const std::string_view numberOfSeenTuplesFieldName)
    : AvgAggregationPhysicalFunction(
          std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier), includeNullValues)
    , numberOfSeenTuplesFieldName(numberOfSeenTuplesFieldName)
{
}

Record
AvgStatisticPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// The average itself is exactly what the base computes; we only re-wrap it and add the count.
    const auto avg = AvgAggregationPhysicalFunction::lower(aggregationState, pipelineMemoryProvider).read(resultFieldIdentifier);
    return makeScalarStatisticRecord(
        resultFieldIdentifier, numberOfSeenTuplesFieldName, avg, resultType, readCount(aggregationState), pipelineMemoryProvider);
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterScalarStatisticAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.numberOfSeenTuplesFieldName.has_value(), "Number of seen tuples is not set");
    INVARIANT(arguments.scalarOp.has_value(), "Scalar statistic op is not set");
    /// The result stamp sizes the payload, and ScalarStatisticLogicalFunction's ctor leaves it VARSIZED until
    /// withInferredStamp has run, which would silently produce a garbage payload width here.
    INVARIANT(arguments.resultType.isNumeric(), "A scalar statistic needs a numeric result stamp, but got {}", arguments.resultType);

    switch (arguments.scalarOp.value())
    {
        case StatisticTuple::StatisticType::Count:
            return std::make_shared<CountStatisticPhysicalFunction>(
                std::move(arguments.inputType),
                std::move(arguments.resultType),
                arguments.inputFunction,
                arguments.resultFieldIdentifier,
                arguments.includeNullValues,
                arguments.numberOfSeenTuplesFieldName.value());
        case StatisticTuple::StatisticType::Sum:
            return std::make_shared<SumStatisticPhysicalFunction>(
                std::move(arguments.inputType),
                std::move(arguments.resultType),
                arguments.inputFunction,
                arguments.resultFieldIdentifier,
                arguments.includeNullValues,
                arguments.numberOfSeenTuplesFieldName.value());
        case StatisticTuple::StatisticType::Avg:
            return std::make_shared<AvgStatisticPhysicalFunction>(
                std::move(arguments.inputType),
                std::move(arguments.resultType),
                arguments.inputFunction,
                arguments.resultFieldIdentifier,
                arguments.includeNullValues,
                arguments.numberOfSeenTuplesFieldName.value());
        default:
            throw UnknownAggregationType(
                "ScalarStatistic expects a scalar statistic type but got {}", magic_enum::enum_name(arguments.scalarOp.value()));
    }
}

}
