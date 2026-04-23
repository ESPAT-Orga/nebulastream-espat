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

WindowStatisticStore::WindowStatisticStore(const uint64_t numberOfExpectedConcurrentAccess)
    : numberOfExpectedConcurrentAccess(numberOfExpectedConcurrentAccess)
{
    allStatistics.reserve(numberOfExpectedConcurrentAccess);
    for (uint64_t i = 0; i < numberOfExpectedConcurrentAccess; ++i)
    {
        allStatistics.emplace_back(folly::Synchronized<IdWindowMap>{});
    }
}

bool WindowStatisticStore::insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic)
{
    const auto startTs = statistic.getStartTs();
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allStatistics[pos].wlock();
    (*lockedStatisticStore)[statisticId][startTs].emplace_back(std::move(statistic));
    return true;
}

bool WindowStatisticStore::deleteStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    bool foundAnyStatistic = false;

    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    auto lockedStatisticStore = allStatistics[pos].wlock();
    const auto idIt = lockedStatisticStore->find(statisticId);
    if (idIt == lockedStatisticStore->end())
    {
        return false;
    }
    auto& windowMap = idIt->second;

    /// Iteration range [lower_bound(startTs), upper_bound(endTs)) covers all keys k with
    /// startTs <= k <= endTs; since key == stat.getStartTs() and stat.getEndTs() >= stat.getStartTs(),
    /// any statistic satisfying the original predicate lives in this range.
    const auto hi = windowMap.upper_bound(endTs);
    for (auto it = windowMap.lower_bound(startTs); it != hi;)
    {
        auto& window = it->second;
        auto newEnd = std::ranges::remove_if(window, [&endTs](const Statistic& curStatistic) { return curStatistic.getEndTs() <= endTs; });

        if (newEnd.begin() != window.end())
        {
            window.erase(newEnd.begin(), newEnd.end());
            foundAnyStatistic = true;
        }
        if (window.empty())
        {
            it = windowMap.erase(it);
        }
        else
        {
            ++it;
        }
    }

    return foundAnyStatistic;
}

std::vector<Statistic> WindowStatisticStore::getStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    std::vector<Statistic> foundStatistics;

    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allStatistics[pos].rlock();
    const auto idIt = lockedStatisticStore->find(statisticId);
    if (idIt == lockedStatisticStore->end())
    {
        return foundStatistics;
    }
    const auto& windowMap = idIt->second;

    const auto lo = windowMap.lower_bound(startTs);
    const auto hi = windowMap.upper_bound(endTs);
    for (auto it = lo; it != hi; ++it)
    {
        std::ranges::copy_if(
            it->second,
            std::back_inserter(foundStatistics),
            [&endTs](const Statistic& curStatistic) { return curStatistic.getEndTs() <= endTs; });
    }
    return foundStatistics;
}

std::optional<Statistic> WindowStatisticStore::getSingleStatistic(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allStatistics[pos].rlock();
    const auto idIt = lockedStatisticStore->find(statisticId);
    if (idIt == lockedStatisticStore->end())
    {
        return {};
    }
    const auto& windowMap = idIt->second;
    const auto wsIt = windowMap.find(startTs);
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
