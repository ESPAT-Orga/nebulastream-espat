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

#include <StatisticStore/DefaultStatisticStore.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <Statistic.hpp>

#include <ranges>
#include <utility>

namespace NES
{

bool DefaultStatisticStore::insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic)
{
    const auto startTs = statistic.getStartTs();
    const auto statisticsLocked = statistics.wlock();
    (*statisticsLocked)[statisticId][startTs].emplace_back(std::move(statistic));
    return true;
}

bool DefaultStatisticStore::deleteStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto statisticsLocked = statistics.wlock();
    const auto idIt = statisticsLocked->find(statisticId);
    if (idIt == statisticsLocked->end())
    {
        return {};
    }
    auto& windowMap = idIt->second;

    bool foundAnyStatistic = false;
    const auto lowerBound = windowMap.lower_bound(startTs);
    const auto higherBound = windowMap.upper_bound(endTs);
    for (auto it = lowerBound; it != higherBound;)
    {
        auto& statisticsVec = it->second;
        const auto range
            = std::ranges::remove_if(statisticsVec, [endTs](const Statistic& statistic) { return statistic.getEndTs() <= endTs; });
        if (range.begin() != statisticsVec.end())
        {
            statisticsVec.erase(range.begin(), statisticsVec.end());
            foundAnyStatistic = true;
        }
        if (statisticsVec.empty())
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

std::vector<Statistic> DefaultStatisticStore::getStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto statisticsLocked = statistics.rlock();
    const auto idIt = statisticsLocked->find(statisticId);
    if (idIt == statisticsLocked->end())
    {
        return {};
    }
    const auto& windowMap = idIt->second;

    std::vector<Statistic> returnStatisticsVector;
    const auto lowerBound = windowMap.lower_bound(startTs);
    const auto higherBound = windowMap.upper_bound(endTs);
    for (auto it = lowerBound; it != higherBound; ++it)
    {
        const auto& statisticsVec = it->second;
        std::ranges::copy_if(
            statisticsVec,
            std::back_inserter(returnStatisticsVector),
            [endTs](const Statistic& statistic) { return statistic.getEndTs() <= endTs; });
    }
    return returnStatisticsVector;
}

std::optional<Statistic> DefaultStatisticStore::getSingleStatistic(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto statisticsLocked = statistics.rlock();
    const auto idIt = statisticsLocked->find(statisticId);
    if (idIt == statisticsLocked->end())
    {
        return std::nullopt;
    }
    const auto& windowMap = idIt->second;
    const auto statisticsVecketIt = windowMap.find(startTs);
    if (statisticsVecketIt == windowMap.end())
    {
        return std::nullopt;
    }
    const auto& statisticsVec = statisticsVecketIt->second;

    const auto it = std::ranges::find_if(
        statisticsVec,
        [startTs, endTs](const Statistic& statistic) { return startTs == statistic.getStartTs() && statistic.getEndTs() == endTs; });
    return it != statisticsVec.end() ? std::make_optional(*it) : std::optional<Statistic>{};
}

std::vector<DefaultStatisticStore::IdStatisticPair> DefaultStatisticStore::getAllStatistics()
{
    std::vector<IdStatisticPair> returnStatisticsVector;
    const auto statisticsLocked = statistics.rlock();

    for (const auto& [statisticId, windowMap] : *statisticsLocked)
    {
        for (const auto& statisticsVecket : windowMap | std::views::values)
        {
            for (const auto& statistic : statisticsVecket)
            {
                returnStatisticsVector.emplace_back(statisticId, statistic);
            }
        }
    }
    return returnStatisticsVector;
}

}
