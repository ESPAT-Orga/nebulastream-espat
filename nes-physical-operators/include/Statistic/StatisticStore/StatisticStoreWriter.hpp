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

#include <optional>
#include <utility>
#include <Interface/Record.hpp>
#include <PhysicalOperator.hpp>
#include <Statistic/StatisticTypes.hpp>

namespace NES
{

/// Persists one statistic per window close.
///
/// This is the only thing in the engine that writes to the statistic store. It is attached as the physical child
/// of the aggregation's probe operator rather than lowered from a logical operator of its own, which is what lets
/// the whole build chain be an ordinary windowed aggregation: AggregationProbePhysicalOperator hands its record
/// straight to executeChild without materialising it, so the payload, the window bounds and the seen-tuple count
/// are all still in registers here and never have to appear in a logical output schema.
///
/// The field names are passed in rather than derived, because the fields it reads are produced by two different
/// things: the window bounds come from the aggregation's window metadata (upstream names them "start"/"end"),
/// while the payload and the tuple count come from ScalarStatisticAggregationPhysicalFunction::lower.
class StatisticStoreWriter final : public PhysicalOperatorConcept
{
public:
    StatisticStoreWriter(
        OperatorHandlerId operatorHandlerId,
        StatisticId statisticId,
        StatisticType statisticType,
        Record::RecordFieldIdentifier dataFieldName,
        Record::RecordFieldIdentifier startTsFieldName,
        Record::RecordFieldIdentifier endTsFieldName,
        Record::RecordFieldIdentifier numberOfSeenTuplesFieldName,
        Record::RecordFieldIdentifier outputStatisticIdFieldName);

    /// Inserts the record's statistic into the store, then forwards the record on with STATISTICID added.
    void execute(ExecutionContext& executionCtx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::optional<PhysicalOperator> child;
    OperatorHandlerId operatorHandlerId;
    StatisticId statisticId;
    StatisticType statisticType;
    Record::RecordFieldIdentifier dataFieldName;
    Record::RecordFieldIdentifier startTsFieldName;
    Record::RecordFieldIdentifier endTsFieldName;
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName;
    Record::RecordFieldIdentifier outputStatisticIdFieldName;
};

}
