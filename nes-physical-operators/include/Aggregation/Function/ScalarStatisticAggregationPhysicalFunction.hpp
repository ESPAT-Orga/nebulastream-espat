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
#include <string>
#include <string_view>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/AvgAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/CountAggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ExecutionContext.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// The physical functions backing the scalar statistics (Count / Sum / Avg). Each reuses the upstream aggregation
/// function that already implements its arithmetic and overrides only lower(), which re-wraps the aggregate into
/// the statistic contract: the number of seen tuples, plus the value as an arena-backed VariableSizedData payload.
/// The payload is the bare value with no header, which is what makes the scalar read path a single read at
/// offset 0 rather than a metadata parse.
///
/// All three share ONE registry name ("ScalarStatistic"); create() picks the class from
/// AggregationPhysicalFunctionRegistryArguments::scalarOp.
///
/// Note the two different notions of "result type" in play. The *logical* aggregate type is VARSIZED, because that
/// is the type of the field the writer consumes. The *physical* resultType handed to these bases is the numeric
/// accumulator type, because it sizes the aggregation state and the payload. create() derives the latter from the
/// op and the input type rather than taking the VARSIZED one it is given.

/// Real COUNT semantics persisted as a statistic. The base's single counter is both the payload and the number of
/// seen tuples.
class CountStatisticPhysicalFunction final : public CountAggregationPhysicalFunction
{
public:
    CountStatisticPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        bool includeNullValues,
        Record::RecordFieldIdentifier numberOfSeenTuplesFieldName);
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    ~CountStatisticPhysicalFunction() override = default;

private:
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName;
};

/// Real SUM semantics persisted as a statistic. Derives from AvgAggregationPhysicalFunction purely for its
/// [sum][count] state: STATISTICNUMBEROFSEENTUPLES needs a tuple counter that SumAggregationPhysicalFunction's
/// state does not carry, and Avg's lift/combine/reset already maintain exactly that pair. This is implementation
/// inheritance, not an is-a relationship. Only lower() differs: it emits the sum itself and skips the division.
class SumStatisticPhysicalFunction final : public AvgAggregationPhysicalFunction
{
public:
    SumStatisticPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        Record::RecordFieldIdentifier numberOfSeenTuplesFieldName);
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    ~SumStatisticPhysicalFunction() override = default;

private:
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName;
};

/// Real AVG semantics persisted as a statistic. The base already computes the quotient, so lower() only re-wraps
/// it and adds the count.
class AvgStatisticPhysicalFunction final : public AvgAggregationPhysicalFunction
{
public:
    AvgStatisticPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        Record::RecordFieldIdentifier numberOfSeenTuplesFieldName);
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    ~AvgStatisticPhysicalFunction() override = default;

private:
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName;
};

/// Factory-only type, never instantiated. It exists because the registry entry is generated as
/// '&${PLUGIN_NAME}AggregationPhysicalFunction::create', so the single name "ScalarStatistic" shared by the three
/// classes above needs one static create to dispatch on arguments.scalarOp.
struct ScalarStatisticAggregationPhysicalFunction
{
    static AggregationPhysicalFunctionRegistryReturnType create(AggregationPhysicalFunctionRegistryArguments arguments);
};

}
