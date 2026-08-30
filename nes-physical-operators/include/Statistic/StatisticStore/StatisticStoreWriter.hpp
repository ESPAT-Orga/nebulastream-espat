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
#include <vector>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <PhysicalOperator.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

class StatisticStoreWriter final : public PhysicalOperatorConcept
{
public:
    /// Persists ONE statistic. The data field to read is the id-derived name resolved during lowering. The
    /// operator forwards its input record (adding only STATISTICID), so chaining N of these persists N synopses.
    explicit StatisticStoreWriter(
        const OperatorHandlerId operatorHandlerId,
        StatisticTuple::StatisticId statisticId,
        const StatisticTuple::StatisticType statisticType,
        Record::RecordFieldIdentifier dataFieldName,
        const LogicalStatisticFields& inputLogicalStatisticFields,
        const LogicalStatisticFields& outputLogicalStatisticFields);

    /// Inserts the given statistic record into the StatisticStore
    void execute(ExecutionContext& executionCtx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::optional<PhysicalOperator> child;
    OperatorHandlerId operatorHandlerId;
    StatisticTuple::StatisticId statisticId;
    StatisticTuple::StatisticType statisticType;
    Record::RecordFieldIdentifier inputStatisticDataFieldName;
    Record::RecordFieldIdentifier inputStatisticStartTsFieldName;
    Record::RecordFieldIdentifier inputStatisticEndTsFieldName;
    Record::RecordFieldIdentifier inputStatisticNumberOfSeenTuplesFieldName;
    Record::RecordFieldIdentifier outputStatisticStartTsFieldName;
    Record::RecordFieldIdentifier outputStatisticEndTsFieldName;
    Record::RecordFieldIdentifier outputStatisticIdFieldName;
    Record::RecordFieldIdentifier outputStatisticNumberOfSeenTuplesFieldName;
};

}
