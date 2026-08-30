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

#include <Interface/NESStrongTypeRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Statistic/StatisticProvider.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Time/Timestamp.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <ExecutionContext.hpp>
#include <StatisticTuple.hpp>
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

/// Thread-local cache populated by loadStatisticsProxy. Holds the statistics matching a single execute() call.
/// Safe to use as TLS because each worker thread executes operator pipelines without re-entrant calls to execute().
thread_local static std::vector<StatisticTuple> tProbeStatistics;

namespace
{
/// Pick the statistic with the highest endTs from a range of stored entries. Returns nullopt if
/// the range is empty. Used by the LATEST_WINDOW_END_SENTINEL fallback path.
std::optional<StatisticTuple> pickLatest(const std::vector<StatisticTuple>& statistics)
{
    if (statistics.empty())
    {
        return std::nullopt;
    }
    return *std::ranges::max_element(statistics, {}, [](const StatisticTuple& s) { return s.getEndTs().getTime(); });
}
}

/// A probe declares what it expects to read -- its statistic type, and for fixed-width payloads their size -- but the
/// store is keyed by statisticId alone, so a probe can reach a statistic that some other build wrote. Reading it anyway
/// would reinterpret the stored bytes (a FLOAT64 average read as a UINT64 sum) or read past the payload (an 8-byte read
/// of a 4-byte sum). Fail loudly instead. expectedPayloadSizeInBytes == 0 means variable-width, so size is unchecked.
static void validateAgainstProbe(
    const std::vector<StatisticTuple>& statistics,
    const StatisticTuple::StatisticId statisticId,
    const StatisticTuple::StatisticType expectedType,
    const uint64_t expectedPayloadSizeInBytes)
{
    for (const auto& statistic : statistics)
    {
        if (statistic.getStatisticType() != expectedType)
        {
            throw CannotProbeStatistic(
                "StatisticTuple {} was built as {} but is probed as {}",
                statisticId,
                magic_enum::enum_name(statistic.getStatisticType()),
                magic_enum::enum_name(expectedType));
        }
        if (expectedPayloadSizeInBytes != 0 and statistic.getStatisticDataSize() != expectedPayloadSizeInBytes)
        {
            throw CannotProbeStatistic(
                "StatisticTuple {} persisted a {}-byte payload but is probed as a {}-byte type",
                statisticId,
                statistic.getStatisticDataSize(),
                expectedPayloadSizeInBytes);
        }
    }
}

static uint64_t loadStatisticsProxy(
    OperatorHandler* ptrOpHandler,
    const StatisticTuple::StatisticId statisticId,
    const Timestamp startTs,
    const Timestamp endTs,
    const StatisticTuple::StatisticType expectedType,
    const uint64_t expectedPayloadSizeInBytes)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    /// LATEST_WINDOW_END_SENTINEL: the caller cannot precompute the just-closed window (live probe),
    /// so walk the full statistic range and keep only the most-recently-closed window.
    if (startTs.getRawValue() == 0 and endTs.getRawValue() == LATEST_WINDOW_END_SENTINEL)
    {
        const auto latest = pickLatest(
            statisticStore->getStatistics(statisticId, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{LATEST_WINDOW_END_SENTINEL}));
        tProbeStatistics.clear();
        if (latest.has_value())
        {
            tProbeStatistics.push_back(latest.value());
        }
        validateAgainstProbe(tProbeStatistics, statisticId, expectedType, expectedPayloadSizeInBytes);
        return tProbeStatistics.size();
    }

    tProbeStatistics = statisticStore->getStatistics(
        statisticId, Windowing::TimeMeasure(startTs.getRawValue()), Windowing::TimeMeasure(endTs.getRawValue()));

    validateAgainstProbe(tProbeStatistics, statisticId, expectedType, expectedPayloadSizeInBytes);
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
    const Record::RecordFieldIdentifier statisticIdFieldName,
    const Record::RecordFieldIdentifier statisticStartTsFieldName,
    const Record::RecordFieldIdentifier statisticEndTsFieldName,
    const Record::RecordFieldIdentifier statisticNumberOfSeenTuplesFieldName,
    StatisticProvider statisticProvider,
    const uint64_t expectedPayloadSizeInBytes)
    : operatorHandlerId(operatorHandlerId)
    , statisticIdFieldName(statisticIdFieldName)
    , statisticStartTsFieldName(statisticStartTsFieldName)
    , statisticEndTsFieldName(statisticEndTsFieldName)
    , statisticNumberOfSeenTuplesFieldName(statisticNumberOfSeenTuplesFieldName)
    , statisticProvider(std::move(statisticProvider))
    , expectedPayloadSizeInBytes(expectedPayloadSizeInBytes)
{
}

void StatisticStoreReader::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Read statistics and call the child with the generated tuples
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const nautilus::val<StatisticTuple::StatisticId> statisticId{
        record.read(statisticIdFieldName).getRawValueAs<nautilus::val<StatisticTuple::StatisticId::Underlying>>()};
    const nautilus::val<Timestamp> startTs{record.read(statisticStartTsFieldName).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};
    const nautilus::val<Timestamp> endTs{record.read(statisticEndTsFieldName).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};

    /// Load all build-window statistics covering [startTs, endTs] into the TLS cache and get their count. The expected
    /// type and payload width let the proxy reject a statistic this probe was not built to read.
    const nautilus::val<StatisticTuple::StatisticType> expectedType{statisticProvider.getStatisticType()};
    const nautilus::val<uint64_t> expectedPayloadSize{expectedPayloadSizeInBytes};
    const auto statisticCount
        = invoke(loadStatisticsProxy, operatorHandlerMemRef, statisticId, startTs, endTs, expectedType, expectedPayloadSize);
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
