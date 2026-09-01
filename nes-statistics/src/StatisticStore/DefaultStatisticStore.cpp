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

#include <algorithm>
#include <iterator>
#include <optional>
#include <utility>
#include <vector>
#include <StatisticTuple.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>

namespace NES
{

bool DefaultStatisticStore::insertStatistic(const StatisticTuple::StatisticId& statisticId, StatisticTuple statistic)
{
    const auto statisticsLocked = statistics.wlock();
    (*statisticsLocked)[statisticId].emplace_back(std::move(statistic));
    return true;
}

bool DefaultStatisticStore::deleteStatistics(
    const StatisticTuple::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto statisticsLocked = statistics.wlock();
    auto& statisticsVec = (*statisticsLocked)[statisticId];

    /// StatisticTuple is not assignable here -- Windowing::TimeMeasure holds a const member -- so its iterators are
    /// not 'permutable' and the usual ranges::remove_if + erase idiom does not compile. Rebuild the vector with
    /// the survivors instead and move-assign it: vector move-assignment steals the buffer and asks nothing of
    /// the elements.
    std::vector<StatisticTuple> keptStatistics;
    keptStatistics.reserve(statisticsVec.size());
    std::ranges::copy_if(
        statisticsVec,
        std::back_inserter(keptStatistics),
        [startTs, endTs](const StatisticTuple& statistic)
        { return not(startTs <= statistic.getStartTs() && statistic.getEndTs() <= endTs); });

    const bool foundAnyStatistic = keptStatistics.size() != statisticsVec.size();
    statisticsVec = std::move(keptStatistics);
    return foundAnyStatistic;
}

std::vector<StatisticTuple> DefaultStatisticStore::getStatistics(
    const StatisticTuple::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto statisticsLocked = statistics.rlock();
    const auto idIt = statisticsLocked->find(statisticId);
    if (idIt == statisticsLocked->end())
    {
        return {};
    }

    std::vector<StatisticTuple> returnStatisticsVector;
    const auto& statisticsVec = idIt->second;
    std::ranges::copy_if(
        statisticsVec,
        std::back_inserter(returnStatisticsVector),
        [startTs, endTs](const StatisticTuple& statistic) { return startTs <= statistic.getStartTs() && statistic.getEndTs() <= endTs; });
    return returnStatisticsVector;
}

std::optional<StatisticTuple> DefaultStatisticStore::getSingleStatistic(
    const StatisticTuple::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto statisticsLocked = statistics.rlock();
    const auto idIt = statisticsLocked->find(statisticId);
    if (idIt == statisticsLocked->end())
    {
        return std::nullopt;
    }
    const auto& statisticsVec = idIt->second;

    const auto it = std::ranges::find_if(
        statisticsVec,
        [startTs, endTs](const StatisticTuple& statistic) { return startTs == statistic.getStartTs() && statistic.getEndTs() == endTs; });
    return it != statisticsVec.end() ? std::make_optional(*it) : std::optional<StatisticTuple>{};
}

std::vector<DefaultStatisticStore::IdStatisticPair> DefaultStatisticStore::getAllStatistics()
{
    std::vector<IdStatisticPair> returnStatisticsVector;
    const auto statisticsLocked = statistics.rlock();

    for (const auto& [statisticId, statisticVec] : *statisticsLocked)
    {
        for (const auto& statistic : statisticVec)
        {
            returnStatisticsVector.emplace_back(statisticId, statistic);
        }
    }
    return returnStatisticsVector;
}

}
