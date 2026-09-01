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

#include <Aggregation/Function/ScalarStatisticAggregationPhysicalFunction.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/Record.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <magic_enum/magic_enum.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

/// The record contract every statistic physical function emits: exactly two fields -- the number of seen tuples,
/// and the payload as arena-backed VariableSizedData. A scalar statistic carries no metadata, so the payload is
/// the bare value with no header.
Record makeScalarStatisticRecord(
    const Record::RecordFieldIdentifier& resultFieldIdentifier,
    const Record::RecordFieldIdentifier& numberOfSeenTuplesFieldName,
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

/// The numeric accumulator type, which sizes both the aggregation state and the persisted payload. Derived from
/// the op rather than taken from the arguments, because the logical function reports VARSIZED there -- that is the
/// type of the field the writer reads, not the type of the value being accumulated.
DataType accumulatorTypeFor(const StatisticType op, const DataType& inputType)
{
    switch (op)
    {
        case StatisticType::Count:
            return DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
        case StatisticType::Avg:
            return DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE);
        case StatisticType::Sum:
            return DataTypeProvider::provideDataType(inputType.type, DataType::NULLABLE::NOT_NULLABLE);
    }
    throw UnknownAggregationType("Unhandled scalar statistic op: {}", magic_enum::enum_name(op));
}

}

CountStatisticPhysicalFunction::CountStatisticPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const bool includeNullValues,
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName)
    : CountAggregationPhysicalFunction(
          std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier), includeNullValues)
    , numberOfSeenTuplesFieldName(std::move(numberOfSeenTuplesFieldName))
{
}

Record CountStatisticPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// The counter is the whole state, so the base reads it and we only re-wrap it. It is both the payload and
    /// the number of seen tuples.
    const auto count
        = CountAggregationPhysicalFunction::lower(aggregationState, parentBuffer, pipelineMemoryProvider).read(resultFieldIdentifier);
    return makeScalarStatisticRecord(resultFieldIdentifier, numberOfSeenTuplesFieldName, count, resultType, count, pipelineMemoryProvider);
}

SumStatisticPhysicalFunction::SumStatisticPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName)
    : AvgAggregationPhysicalFunction(
          std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , numberOfSeenTuplesFieldName(std::move(numberOfSeenTuplesFieldName))
{
}

Record SumStatisticPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, nautilus::val<TupleBuffer*>, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Unlike the base we emit the running sum verbatim rather than dividing it by the count; the count becomes
    /// the number of seen tuples instead.
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
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName)
    : AvgAggregationPhysicalFunction(
          std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , numberOfSeenTuplesFieldName(std::move(numberOfSeenTuplesFieldName))
{
}

Record AvgStatisticPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// The average itself is exactly what the base computes; we only re-wrap it and add the count.
    const auto avg
        = AvgAggregationPhysicalFunction::lower(aggregationState, parentBuffer, pipelineMemoryProvider).read(resultFieldIdentifier);
    return makeScalarStatisticRecord(
        resultFieldIdentifier, numberOfSeenTuplesFieldName, avg, resultType, readCount(aggregationState), pipelineMemoryProvider);
}

AggregationPhysicalFunctionRegistryReturnType
ScalarStatisticAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.scalarOp.has_value(), "Scalar statistic op is not set");
    const auto op = arguments.scalarOp.value();
    auto accumulatorType = accumulatorTypeFor(op, arguments.inputType);
    const Record::RecordFieldIdentifier numberOfSeenTuplesFieldName{
        Identifier::parse(std::string{StatisticFieldNames::NUMBER_OF_SEEN_TUPLES})};

    switch (op)
    {
        case StatisticType::Count:
            return std::make_shared<CountStatisticPhysicalFunction>(
                std::move(arguments.inputType),
                std::move(accumulatorType),
                arguments.inputFunction,
                arguments.resultFieldIdentifier,
                arguments.includeNullValues,
                numberOfSeenTuplesFieldName);
        case StatisticType::Sum:
            return std::make_shared<SumStatisticPhysicalFunction>(
                std::move(arguments.inputType),
                std::move(accumulatorType),
                arguments.inputFunction,
                arguments.resultFieldIdentifier,
                numberOfSeenTuplesFieldName);
        case StatisticType::Avg:
            return std::make_shared<AvgStatisticPhysicalFunction>(
                std::move(arguments.inputType),
                std::move(accumulatorType),
                arguments.inputFunction,
                arguments.resultFieldIdentifier,
                numberOfSeenTuplesFieldName);
    }
    throw UnknownAggregationType("Unhandled scalar statistic op: {}", magic_enum::enum_name(op));
}

}
