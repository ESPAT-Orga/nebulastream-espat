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
#include <string>
#include <string_view>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/AvgAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/CountAggregationPhysicalFunction.hpp>

namespace NES
{

/// The physical functions backing the scalar statistics (Count / Sum / Avg). Each one reuses the aggregation
/// physical function that already implements its arithmetic and overrides only lower(), which re-wraps the
/// aggregate as the statistic contract: the number of seen tuples plus the value as a VariableSizedData payload.
/// Unlike the synopsis statistics (CountMinSketch / EquiWidthHistogram / ReservoirSample) they carry no memory
/// budget and no payload metadata, so the payload is the bare value (see ScalarStatisticIteratorImpl).
/// They share ONE plugin name ("ScalarStatistic"); the registrar picks the class from the op.

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
        std::string_view numberOfSeenTuplesFieldName);
    Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    ~CountStatisticPhysicalFunction() override = default;

private:
    std::string numberOfSeenTuplesFieldName;
};

/// Real SUM semantics persisted as a statistic. We derive from AvgAggregationPhysicalFunction purely for its
/// [sum][count] state: STATISTICNUMBEROFSEENTUPLES needs a tuple counter that SumAggregationPhysicalFunction's
/// state does not carry, and Avg's lift/combine/reset already maintain exactly that pair. This is implementation
/// inheritance, not an is-a relationship. Only lower() differs: we emit the sum itself and skip the division.
class SumStatisticPhysicalFunction final : public AvgAggregationPhysicalFunction
{
public:
    SumStatisticPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        bool includeNullValues,
        std::string_view numberOfSeenTuplesFieldName);
    Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    ~SumStatisticPhysicalFunction() override = default;

private:
    std::string numberOfSeenTuplesFieldName;
};

/// Real AVG semantics persisted as a statistic. The base already computes the quotient, so lower() only re-wraps it
/// and adds the count, which is the tuple count because we are registered with includeNullValues = true.
class AvgStatisticPhysicalFunction final : public AvgAggregationPhysicalFunction
{
public:
    AvgStatisticPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        bool includeNullValues,
        std::string_view numberOfSeenTuplesFieldName);
    Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    ~AvgStatisticPhysicalFunction() override = default;

private:
    std::string numberOfSeenTuplesFieldName;
};

}
