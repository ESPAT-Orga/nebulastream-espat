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

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string_view>
#include <vector>

#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Statistic/StatisticProvider.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Time/Timestamp.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <ExecutionContext.hpp>
#include <Statistic.hpp>
#include <function.hpp>
#include <val_arith.hpp>

namespace NES
{

/// Sentinel meaning "look up the most-recently-closed window for this statisticId" rather than
/// requiring an exact (startTs, endTs) match. Used by live-deployed probe pipelines where the
/// caller cannot precompute which window has just closed — the build branch and probe run
/// concurrently. The driving Generator emits (statisticId, 0, LATEST_WINDOW_END_SENTINEL); the
/// reader detects this pair and walks the full statistic range via getStatistics() to pick the
/// latest. We use UINT64_MAX - 1 (not UINT64_MAX itself) so a GeneratorSource SequenceField with
/// start = sentinel, end = sentinel + 1, step = 0 can emit it directly (SequenceField needs
/// end > start to leave room for sequencePosition).
constexpr uint64_t LATEST_WINDOW_END_SENTINEL = std::numeric_limits<uint64_t>::max() - 1;

namespace
{
/// Pick the statistic with the highest endTs from a range of stored entries. Returns nullopt if
/// the range is empty. Used by the LATEST_WINDOW_END_SENTINEL fallback path.
std::optional<Statistic> pickLatest(const std::vector<Statistic>& statistics)
{
    if (statistics.empty())
    {
        return std::nullopt;
    }
    return *std::ranges::max_element(statistics, {}, [](const Statistic& s) { return s.getEndTs().getTime(); });
}

std::optional<Statistic>
resolveStatistic(AbstractStatisticStore& store, const Statistic::StatisticId statisticId, const Timestamp startTs, const Timestamp endTs)
{
    if (startTs.getRawValue() == 0 and endTs.getRawValue() == LATEST_WINDOW_END_SENTINEL)
    {
        return pickLatest(store.getStatistics(statisticId, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{LATEST_WINDOW_END_SENTINEL}));
    }
    return store.getSingleStatistic(
        statisticId, Windowing::TimeMeasure(startTs.getRawValue()), Windowing::TimeMeasure(endTs.getRawValue()));
}
}

const static int8_t* getStatisticDataProxy(
    OperatorHandler* ptrOpHandler, const Statistic::StatisticId statisticId, const Timestamp startTs, const Timestamp endTs)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    if (const auto statistic = resolveStatistic(*statisticStore, statisticId, startTs, endTs); statistic.has_value())
    {
        return statistic.value().getStatisticData();
    }
    return nullptr;
}

uint64_t getNumberOfSeenTuplesOfStatistic(
    OperatorHandler* ptrOpHandler, const Statistic::StatisticId statisticId, const Timestamp startTs, const Timestamp endTs)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    if (const auto statistic = resolveStatistic(*statisticStore, statisticId, startTs, endTs); statistic.has_value())
    {
        return statistic.value().getNumberOfSeenTuples();
    }
    return 0;
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
    const auto numberOfSeenTuples = invoke(getNumberOfSeenTuplesOfStatistic, operatorHandlerMemRef, statisticId, startTs, endTs);
    const auto statisticMemArea = invoke(getStatisticDataProxy, operatorHandlerMemRef, statisticId, startTs, endTs);
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

std::optional<PhysicalOperator> StatisticStoreReader::getChild() const
{
    return child;
}

void StatisticStoreReader::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
