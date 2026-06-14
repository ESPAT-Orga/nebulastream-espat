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

#include <cstdint>
#include <string_view>

#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Statistic/StatisticProvider.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <Statistic.hpp>
#include <function.hpp>
#include <val_arith.hpp>

namespace NES
{

/// Thread-local cache populated by loadStatisticsProxy. Holds the statistics matching a single execute() call.
/// Safe to use as TLS because each worker thread executes operator pipelines without re-entrant calls to execute().
thread_local static std::vector<Statistic> tProbeStatistics;

static uint64_t
loadStatisticsProxy(OperatorHandler* ptrOpHandler, const Statistic::StatisticId statisticId, const Timestamp startTs, const Timestamp endTs)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    tProbeStatistics = statisticStore->getStatistics(
        statisticId, Windowing::TimeMeasure(startTs.getRawValue()), Windowing::TimeMeasure(endTs.getRawValue()));

    return tProbeStatistics.size();
}

static const int8_t* getStatisticDataByIndexProxy(const uint64_t index)
{
    if (index >= tProbeStatistics.size())
    {
        return nullptr;
    }
    return tProbeStatistics[index].getStatisticData();
}

static uint64_t getSeenTuplesByIndexProxy(const uint64_t index)
{
    if (index >= tProbeStatistics.size())
    {
        return 0;
    }
    return tProbeStatistics[index].getNumberOfSeenTuples();
}

StatisticStoreReader::StatisticStoreReader(
    const OperatorHandlerId operatorHandlerId,
    const std::string_view statisticIdFieldName,
    const std::string_view statisticStartTsFieldName,
    const std::string_view statisticEndTsFieldName,
    const std::string_view statisticNumberOfSeenTuplesFieldName,
    StatisticProvider statisticProvider)
    : operatorHandlerId(operatorHandlerId)
    , statisticIdFieldName(statisticIdFieldName)
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
    const nautilus::val<Statistic::StatisticId> statisticId{
        record.read(statisticIdFieldName).getRawValueAs<nautilus::val<Statistic::StatisticId::Underlying>>()};
    const nautilus::val<Timestamp> startTs{record.read(statisticStartTsFieldName).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};
    const nautilus::val<Timestamp> endTs{record.read(statisticEndTsFieldName).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};

    /// Load all build-window statistics covering [startTs, endTs] into the TLS cache and get their count.
    const auto statisticCount = invoke(loadStatisticsProxy, operatorHandlerMemRef, statisticId, startTs, endTs);
    for (nautilus::val<uint64_t> i = 0; i < statisticCount; ++i)
    {
        const auto numberOfSeenTuples = invoke(getSeenTuplesByIndexProxy, i);
        const auto statisticMemArea = invoke(getStatisticDataByIndexProxy, i);
        if (statisticMemArea != nullptr)
        {
            for (auto statisticIterator = statisticProvider.begin(statisticMemArea);
                 statisticIterator != statisticProvider.end(statisticMemArea);
                 ++statisticIterator)
            {
                /// Getting a record containing the data from the current statistic, e.g., for a histogram the upper, lower bound and counter
                Record statisticRecord = *statisticIterator;

                /// Adding additional data so that downstream operators know when and for what the statistic was created
                statisticRecord.write(statisticStartTsFieldName, startTs.convertToValue());
                statisticRecord.write(statisticEndTsFieldName, endTs.convertToValue());
                statisticRecord.write(statisticIdFieldName, statisticId.convertToValue());
                statisticRecord.write(statisticNumberOfSeenTuplesFieldName, numberOfSeenTuples);
                executeChild(executionCtx, statisticRecord);
            }
        }
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
