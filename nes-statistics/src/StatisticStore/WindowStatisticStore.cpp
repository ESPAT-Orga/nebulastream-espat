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
#include <StatisticStore/WindowStatisticStore.hpp>

#include <StatisticStore/AbstractStatisticStore.hpp>
#include <Statistic.hpp>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <ranges>
#include <utility>

namespace NES
{

namespace
{
uint64_t getPos(const Statistic::StatisticId& statisticId, const uint64_t numberOfExpectedConcurrentAccess)
{
    /// Shard the outer map by StatisticId only: all windows for a given id live in the same sub-store,
    /// so range queries acquire a single rlock and walk a compact per-id inner map.
    /// We can not use a worker thread id or etc, as this function is not only called from the execution.
    return std::hash<Statistic::StatisticId>{}(statisticId) % numberOfExpectedConcurrentAccess;
}
}

WindowStatisticStore::WindowStatisticStore(const uint64_t numberOfExpectedConcurrentAccess, const Windowing::TimeMeasure windowSize)
    : numberOfExpectedConcurrentAccess(numberOfExpectedConcurrentAccess), windowSize(windowSize)
{
    allStatistics.reserve(numberOfExpectedConcurrentAccess);
    for (uint64_t i = 0; i < numberOfExpectedConcurrentAccess; ++i)
    {
        allStatistics.emplace_back(folly::Synchronized<IdWindowMap>{});
    }
}

Windowing::TimeMeasure WindowStatisticStore::calculateWindowStartTime(const Windowing::TimeMeasure statStartTime) const
{
    const auto startTimeRawValue = statStartTime.getTime();
    const uint64_t windowStartTime = windowSize.getTime() * std::floor((startTimeRawValue / windowSize.getTime()));
    return Windowing::TimeMeasure{windowStartTime};
}

bool WindowStatisticStore::insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic)
{
    const auto windowStartTime = calculateWindowStartTime(statistic.getStartTs());
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allStatistics[pos].wlock();
    (*lockedStatisticStore)[statisticId][windowStartTime].emplace_back(std::move(statistic));
    return true;
}

bool WindowStatisticStore::deleteStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const uint64_t firstWindow = std::floor(startTs.getTime() / windowSize.getTime());
    const uint64_t lastWindow = std::floor(endTs.getTime() / windowSize.getTime());
    const auto numberOfWindows = lastWindow - firstWindow + 1;
    bool foundAnyStatistic = false;

    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    auto lockedStatisticStore = allStatistics[pos].wlock();
    const auto idIt = lockedStatisticStore->find(statisticId);
    if (idIt == lockedStatisticStore->end())
    {
        return false;
    }
    auto& windowMap = idIt->second;

    for (uint64_t i = 0; i < numberOfWindows; ++i)
    {
        const Windowing::TimeMeasure curWindowStartTime{firstWindow * windowSize.getTime() + i * windowSize.getTime()};
        const auto wsIt = windowMap.find(curWindowStartTime);
        if (wsIt == windowMap.end())
        {
            continue;
        }
        auto& window = wsIt->second;
        auto newEnd = std::ranges::remove_if(
            window,
            [startTs, endTs](const Statistic& curStatistic)
            { return curStatistic.getStartTs() >= startTs and curStatistic.getEndTs() <= endTs; });

        if (newEnd.begin() != window.end())
        {
            window.erase(newEnd.begin(), newEnd.end());
            foundAnyStatistic = true;
        }
    }

    return foundAnyStatistic;
}

std::vector<Statistic> WindowStatisticStore::getStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const uint64_t firstWindow = std::floor(startTs.getTime() / windowSize.getTime());
    const uint64_t lastWindow = std::floor(endTs.getTime() / windowSize.getTime());
    const auto numberOfWindows = lastWindow - firstWindow + 1;
    std::vector<Statistic> foundStatistics;

    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allStatistics[pos].rlock();
    const auto idIt = lockedStatisticStore->find(statisticId);
    if (idIt == lockedStatisticStore->end())
    {
        return foundStatistics;
    }
    const auto& windowMap = idIt->second;

    for (uint64_t i = 0; i < numberOfWindows; ++i)
    {
        const Windowing::TimeMeasure curWindowStartTime{firstWindow * windowSize.getTime() + i * windowSize.getTime()};
        const auto wsIt = windowMap.find(curWindowStartTime);
        if (wsIt == windowMap.end())
        {
            continue;
        }
        std::ranges::copy_if(
            wsIt->second,
            std::back_inserter(foundStatistics),
            [startTs, endTs](const Statistic& curStatistic)
            { return curStatistic.getStartTs() >= startTs and curStatistic.getEndTs() <= endTs; });
    }
    return foundStatistics;
}

std::optional<Statistic> WindowStatisticStore::getSingleStatistic(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const uint64_t firstWindow = std::floor(startTs.getTime() / windowSize.getTime());
    const Windowing::TimeMeasure curWindowStartTime{firstWindow * windowSize.getTime()};

    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allStatistics[pos].rlock();
    const auto idIt = lockedStatisticStore->find(statisticId);
    if (idIt == lockedStatisticStore->end())
    {
        return {};
    }
    const auto& windowMap = idIt->second;
    const auto wsIt = windowMap.find(curWindowStartTime);
    if (wsIt == windowMap.end())
    {
        return {};
    }
    const auto& window = wsIt->second;
    const auto foundStatistic = std::ranges::find_if(
        window,
        [startTs, endTs](const Statistic& curStatistic)
        { return curStatistic.getStartTs() == startTs and curStatistic.getEndTs() == endTs; });
    if (foundStatistic != window.end())
    {
        return *foundStatistic;
    }

    return {};
}

std::vector<AbstractStatisticStore::IdStatisticPair> WindowStatisticStore::getAllStatistics()
{
    std::vector<AbstractStatisticStore::IdStatisticPair> retStatistics;
    for (auto& statisticStore : allStatistics)
    {
        auto lockedStatisticStore = statisticStore.rlock();
        for (const auto& [statisticId, windowMap] : *lockedStatisticStore)
        {
            for (const auto& [windowStart, window] : windowMap)
            {
                for (const auto& statistic : window)
                {
                    retStatistics.emplace_back(statisticId, statistic);
                }
            }
        }
    }

    return retStatistics;
}

}
