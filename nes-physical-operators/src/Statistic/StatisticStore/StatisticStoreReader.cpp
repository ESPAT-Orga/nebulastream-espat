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
#include <Statistic/StatisticStore/StatisticStoreReader.hpp>

#include <Statistic/StatisticProvider.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

const int8_t*
getStatisticDataProxy(OperatorHandler* ptrOpHandler, const Statistic::StatisticHash hash, const Timestamp startTs, const Timestamp endTs)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    const auto statistic = statisticStore->getSingleStatistic(
        hash, Windowing::TimeMeasure(startTs.getRawValue()), Windowing::TimeMeasure(endTs.getRawValue()));

    if (statistic.has_value())
    {
        return statistic.value()->getStatisticData();
    }
    return nullptr;
}

uint64_t getNumberOfSeenTuplesOfStatistic(
    OperatorHandler* ptrOpHandler, const Statistic::StatisticHash hash, const Timestamp startTs, const Timestamp endTs)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    const auto statistic = statisticStore->getSingleStatistic(
        hash, Windowing::TimeMeasure(startTs.getRawValue()), Windowing::TimeMeasure(endTs.getRawValue()));

    if (statistic.has_value())
    {
        return statistic.value()->getNumberOfSeenTuples();
    }
    return 0;
}

StatisticStoreReader::StatisticStoreReader(
    const OperatorHandlerId operatorHandlerId,
    const std::string_view statisticHashFieldName,
    const std::string_view statisticStartTsFieldName,
    const std::string_view statisticEndTsFieldName,
    const std::string_view statisticNumberOfSeenTuplesFieldName,
    StatisticProvider statisticProvider)
    : operatorHandlerId(operatorHandlerId)
    , statisticHashFieldName(statisticHashFieldName)
    , statisticStartTsFieldName(statisticStartTsFieldName)
    , statisticEndTsFieldName(statisticEndTsFieldName)
    , statisticNumberOfSeenTuplesFieldName(statisticNumberOfSeenTuplesFieldName)
    , statisticProvider(std::move(statisticProvider))
{
}

void StatisticStoreReader::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Read statistics and call the child with the generated tuples
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto statisticHash = record.read(statisticHashFieldName).cast<nautilus::val<Statistic::StatisticHash>>();
    const nautilus::val<Timestamp> startTs{record.read(statisticStartTsFieldName).cast<nautilus::val<Timestamp::Underlying>>()};
    const nautilus::val<Timestamp> endTs{record.read(statisticEndTsFieldName).cast<nautilus::val<Timestamp::Underlying>>()};
    const auto numberOfSeenTuples = invoke(getNumberOfSeenTuplesOfStatistic, operatorHandlerMemRef, statisticHash, startTs, endTs);
    const auto statisticMemArea = invoke(getStatisticDataProxy, operatorHandlerMemRef, statisticHash, startTs, endTs);
    for (auto statisticIterator = statisticProvider.begin(statisticMemArea); statisticIterator != statisticProvider.end(statisticMemArea);
         ++statisticIterator)
    {
        /// Getting a record containing the data from the current statistic, e.g., for a histogram the upper, lower bound and counter
        Record statisticRecord = *statisticIterator;

        /// Adding additional data so that downstream operators know when and for what the statistic was created
        statisticRecord.write(statisticStartTsFieldName, startTs.convertToValue());
        statisticRecord.write(statisticEndTsFieldName, endTs.convertToValue());
        statisticRecord.write(statisticHashFieldName, statisticHash);
        statisticRecord.write(statisticNumberOfSeenTuplesFieldName, numberOfSeenTuples);
        executeChild(executionCtx, statisticRecord);
    }
}

std::optional<PhysicalOperator> StatisticStoreReader::getChild() const
{
    return child;
}

void StatisticStoreReader::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
