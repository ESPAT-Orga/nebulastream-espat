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
    const auto locked = statistics.wlock();
    (*locked)[statisticId][startTs].emplace_back(std::move(statistic));
    return true;
}

bool DefaultStatisticStore::deleteStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto locked = statistics.wlock();
    const auto idIt = locked->find(statisticId);
    if (idIt == locked->end())
    {
        return false;
    }
    auto& windowMap = idIt->second;

    bool foundAny = false;
    const auto hi = windowMap.upper_bound(endTs);
    for (auto it = windowMap.lower_bound(startTs); it != hi;)
    {
        auto& vec = it->second;
        const auto removed = std::ranges::remove_if(vec, [&endTs](const Statistic& s) { return s.getEndTs() <= endTs; });
        if (removed.begin() != vec.end())
        {
            vec.erase(removed.begin(), vec.end());
            foundAny = true;
        }
        if (vec.empty())
            it = windowMap.erase(it);
        else
            ++it;
    }
    return foundAny;
}

std::vector<Statistic> DefaultStatisticStore::getStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto locked = statistics.rlock();
    const auto idIt = locked->find(statisticId);
    if (idIt == locked->end())
    {
        return {};
    }
    const auto& windowMap = idIt->second;

    std::vector<Statistic> result;
    const auto lo = windowMap.lower_bound(startTs);
    const auto hi = windowMap.upper_bound(endTs);
    for (auto it = lo; it != hi; ++it)
    {
        std::ranges::copy_if(
            it->second, std::back_inserter(result), [&endTs](const Statistic& s) { return s.getEndTs() <= endTs; });
    }
    return result;
}

std::optional<Statistic> DefaultStatisticStore::getSingleStatistic(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto locked = statistics.rlock();
    const auto idIt = locked->find(statisticId);
    if (idIt == locked->end())
    {
        return std::nullopt;
    }
    const auto& windowMap = idIt->second;
    const auto wsIt = windowMap.find(startTs);
    if (wsIt == windowMap.end())
    {
        return std::nullopt;
    }
    const auto& vec = wsIt->second;
    const auto found = std::ranges::find_if(
        vec, [&](const Statistic& s) { return s.getStartTs() == startTs && s.getEndTs() == endTs; });
    return found != vec.end() ? std::make_optional(*found) : std::nullopt;
}

std::vector<DefaultStatisticStore::IdStatisticPair> DefaultStatisticStore::getAllStatistics()
{
    std::vector<IdStatisticPair> result;
    const auto locked = statistics.rlock();
    for (const auto& [id, windowMap] : *locked)
    {
        for (const auto& [ts, vec] : windowMap)
        {
            for (const auto& stat : vec)
                result.emplace_back(id, stat);
        }
    }
    return result;
}

}
