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
#include <Aggregation/Function/AvgAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/CountAggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// The physical functions backing the scalar statistics (Count / Sum / Avg). Each one reuses the aggregation
/// physical function that already implements its arithmetic and overrides only lower(), which re-wraps the
/// aggregate as the statistic contract: the number of seen tuples plus the value as a VariableSizedData payload.
/// Unlike the synopsis statistics (CountMinSketch / EquiWidthHistogram / ReservoirSample) they carry no memory
/// budget and no payload metadata, so the payload is the bare value (see ScalarStatisticIteratorImpl).
/// They share ONE registry entry, ScalarStatistic; the factory picks the class from the op.

/// Real COUNT semantics persisted as a statistic. The base's single counter is both the payload and the number of
/// seen tuples, because we are registered with includeNullValues = true and so lift() counts every tuple.
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
        nautilus::val<TupleBuffer*> tupleBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    ~CountStatisticPhysicalFunction() override = default;

private:
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName;
};

/// Real SUM semantics persisted as a statistic. We derive from AvgAggregationPhysicalFunction purely for its
/// [sum][count] state: STATISTICNUMBEROFSEENTUPLES needs a tuple counter that SumAggregationPhysicalFunction's
/// state does not carry, and Avg's lift/combine/reset already maintain exactly that pair. This is implementation
/// inheritance, not an is-a relationship. Only lower() differs: we emit the sum itself and skip the division.
///
/// TODO: revisit null handling. statistic-renaming passes includeNullValues down to
/// AvgAggregationPhysicalFunction, whose constructor took it. Upstream's Avg neither takes nor stores the flag --
/// it derives its behaviour from inputType.nullable and applies SQL-standard AVG semantics (NULL inputs skipped
/// from both sum and count), and its own create() discards arguments.includeNullValues. We therefore drop the
/// flag here rather than invent a second null policy. This matches upstream Avg exactly, but it means a scalar
/// Sum/Avg statistic over a nullable field counts only non-NULL inputs in STATISTICNUMBEROFSEENTUPLES, whereas
/// the Count statistic (whose base does take the flag, and is registered with it true) counts every tuple. Decide
/// whether that asymmetry is intended before the scalar statistics are used on nullable fields.
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
        nautilus::val<TupleBuffer*> tupleBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    ~SumStatisticPhysicalFunction() override = default;

private:
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName;
};

/// Real AVG semantics persisted as a statistic. The base already computes the quotient, so lower() only re-wraps it
/// and adds the count. See the null-handling TODO on SumStatisticPhysicalFunction, which applies here too.
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
        nautilus::val<TupleBuffer*> tupleBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    ~AvgStatisticPhysicalFunction() override = default;

private:
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName;
};

/// Factory for the shared ScalarStatistic registry entry. Not an aggregation itself: it exists because upstream's
/// registry keys one entry to one `<PluginName>AggregationPhysicalFunction::create`, while the three classes above
/// share a single plugin name and are selected by arguments.scalarOp.
struct ScalarStatisticAggregationPhysicalFunction
{
    static AggregationPhysicalFunctionRegistryReturnType create(AggregationPhysicalFunctionRegistryArguments arguments);
};

}
