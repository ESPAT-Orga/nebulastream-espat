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

#include <StatisticStore/SubStoresStatisticStore.hpp>

#include <StatisticStore/AbstractStatisticStore.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <Statistic.hpp>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iterator>
#include <ranges>
#include <thread>
#include <utility>

namespace NES
{
namespace
{
uint64_t getPos(const uint64_t numberOfExpectedConcurrentAccess)
{
    /// We use the thread id hash to distribute accesses across sub stores.
    /// We can not use a worker thread id or etc, as this function is not only called from the execution.
    /// The hash is cached per-thread; the modulo is recomputed so different store instances
    /// (with potentially different sub-store counts) stay in range on the same thread.
    thread_local const auto threadHash = static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    return threadHash % numberOfExpectedConcurrentAccess;
}
}

SubStoresStatisticStore::SubStoresStatisticStore(const uint64_t numberOfExpectedConcurrentAccess)
    : numberOfExpectedConcurrentAccess(numberOfExpectedConcurrentAccess)
{
    allSubStores.reserve(numberOfExpectedConcurrentAccess);
    for (uint64_t i = 0; i < numberOfExpectedConcurrentAccess; ++i)
    {
        allSubStores.emplace_back(folly::Synchronized<IdWindowMap>{});
    }
}

bool SubStoresStatisticStore::insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic)
{
    const auto startTs = statistic.getStartTs();
    const auto pos = getPos(numberOfExpectedConcurrentAccess);
    const auto locked = allSubStores[pos].wlock();
    (*locked)[statisticId][startTs].emplace_back(std::move(statistic));
    return true;
}

bool SubStoresStatisticStore::deleteStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    bool foundAny = false;
    for (auto& subStore : allSubStores)
    {
        const auto locked = subStore.wlock();
        const auto idIt = locked->find(statisticId);
        if (idIt == locked->end())
            continue;
        auto& windowMap = idIt->second;

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
    }
    return foundAny;
}

std::vector<Statistic> SubStoresStatisticStore::getStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    std::vector<Statistic> result;
    for (const auto& subStore : allSubStores)
    {
        const auto locked = subStore.rlock();
        const auto idIt = locked->find(statisticId);
        if (idIt == locked->end())
            continue;
        const auto& windowMap = idIt->second;

        const auto lo = windowMap.lower_bound(startTs);
        const auto hi = windowMap.upper_bound(endTs);
        for (auto it = lo; it != hi; ++it)
        {
            std::ranges::copy_if(
                it->second, std::back_inserter(result), [&endTs](const Statistic& s) { return s.getEndTs() <= endTs; });
        }
    }
    return result;
}

std::optional<Statistic> SubStoresStatisticStore::getSingleStatistic(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    for (const auto& subStore : allSubStores)
    {
        const auto locked = subStore.rlock();
        const auto idIt = locked->find(statisticId);
        if (idIt == locked->end())
            continue;
        const auto& windowMap = idIt->second;
        const auto wsIt = windowMap.find(startTs);
        if (wsIt == windowMap.end())
            continue;
        const auto& vec = wsIt->second;
        const auto found = std::ranges::find_if(
            vec, [&](const Statistic& s) { return s.getStartTs() == startTs && s.getEndTs() == endTs; });
        if (found != vec.end())
            return *found;
    }
    return {};
}

std::vector<AbstractStatisticStore::IdStatisticPair> SubStoresStatisticStore::getAllStatistics()
{
    std::vector<IdStatisticPair> result;
    for (const auto& subStore : allSubStores)
    {
        const auto locked = subStore.rlock();
        for (const auto& [id, windowMap] : *locked)
        {
            for (const auto& [ts, vec] : windowMap)
            {
                for (const auto& stat : vec)
                    result.emplace_back(id, stat);
            }
        }
    }
    return result;
}

}
