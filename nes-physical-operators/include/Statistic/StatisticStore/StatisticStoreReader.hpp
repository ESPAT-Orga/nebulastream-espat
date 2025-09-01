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

#include <Statistic/StatisticProvider.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// @brief Creates a new "stream" by reading a particular statistic from the store and converts it
/// to a stream that then can be run queries over it.
/// For example, a sample would return all of its tuples whereas a histogram would create tuples for each bin.
class StatisticStoreReader final : public PhysicalOperatorConcept
{
public:
    explicit StatisticStoreReader(
        OperatorHandlerId operatorHandlerId,
        std::string_view statisticHashFieldName,
        std::string_view statisticStartTsFieldName,
        std::string_view statisticEndTsFieldName,
        std::string_view statisticNumberOfSeenTuplesFieldName,
        StatisticProvider statisticProvider);

    /// Gets the single statistic for given metadata from the StatisticStore and creates records out of it
    void execute(ExecutionContext& executionCtx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::optional<PhysicalOperator> child;
    OperatorHandlerId operatorHandlerId;
    std::string statisticHashFieldName;
    std::string statisticStartTsFieldName;
    std::string statisticEndTsFieldName;
    std::string statisticNumberOfSeenTuplesFieldName;
    StatisticProvider statisticProvider;
};

}
