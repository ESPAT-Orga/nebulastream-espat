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

#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <PhysicalOperator.hpp>
#include <Statistic.hpp>

namespace NES
{

class StatisticStoreWriter final : public PhysicalOperatorConcept
{
public:
    explicit StatisticStoreWriter(
        const OperatorHandlerId operatorHandlerId,
        const Statistic::StatisticHash statisticHash,
        const Statistic::StatisticType statisticType,
        const LogicalStatisticFields& inputLogicalStatisticFields,
        const LogicalStatisticFields& outputLogicalStatisticFields);

    /// Inserts the given statistic record into the StatisticStore
    void execute(ExecutionContext& executionCtx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::optional<PhysicalOperator> child;
    OperatorHandlerId operatorHandlerId;
    Statistic::StatisticHash statisticHash;
    Statistic::StatisticType statisticType;
    std::string inputStatisticStartTsFieldName;
    std::string inputStatisticEndTsFieldName;
    std::string inputStatisticDataFieldName;
    std::string inputStatisticNumberOfSeenTuplesFieldName;
    std::string outputStatisticStartTsFieldName;
    std::string outputStatisticEndTsFieldName;
    std::string outputStatisticHashFieldName;
    std::string outputStatisticNumberOfSeenTuplesFieldName;
};

}
