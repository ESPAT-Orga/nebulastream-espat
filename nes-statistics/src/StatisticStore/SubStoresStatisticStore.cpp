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
#include <unordered_map>
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
        allSubStores.emplace_back(folly::Synchronized<std::unordered_map<Statistic::StatisticId, std::vector<Statistic>>>{});
    }
}

bool SubStoresStatisticStore::insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic)
{
    const auto pos = getPos(numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allSubStores[pos].wlock();
    (*lockedStatisticStore)[statisticId].emplace_back(std::move(statistic));
    return true;
}

bool SubStoresStatisticStore::deleteStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    bool foundAnyStatistic = false;
    for (auto& statisticStore : allSubStores)
    {
        const auto lockedStatisticStore = statisticStore.wlock();
        const auto bucketIt = lockedStatisticStore->find(statisticId);
        if (bucketIt == lockedStatisticStore->end())
        {
            continue;
        }
        auto& bucket = bucketIt->second;
        const auto removed = std::ranges::remove_if(
            bucket,
            [startTs, endTs](const Statistic& statistic) { return statistic.getStartTs() >= startTs and statistic.getEndTs() <= endTs; });
        if (removed.begin() != bucket.end())
        {
            bucket.erase(removed.begin(), removed.end());
            foundAnyStatistic = true;
        }
    }
    return foundAnyStatistic;
}

std::vector<Statistic> SubStoresStatisticStore::getStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    std::vector<Statistic> foundStatistics;
    for (const auto& statisticStore : allSubStores)
    {
        const auto lockedStatisticStore = statisticStore.rlock();
        const auto bucketIt = lockedStatisticStore->find(statisticId);
        if (bucketIt == lockedStatisticStore->end())
        {
            continue;
        }
        std::ranges::copy_if(
            bucketIt->second,
            std::back_inserter(foundStatistics),
            [startTs, endTs](const Statistic& statistic) { return statistic.getStartTs() >= startTs and statistic.getEndTs() <= endTs; });
    }
    return foundStatistics;
}

std::optional<Statistic> SubStoresStatisticStore::getSingleStatistic(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    for (const auto& statisticStore : allSubStores)
    {
        const auto lockedStatisticStore = statisticStore.rlock();
        const auto bucketIt = lockedStatisticStore->find(statisticId);
        if (bucketIt == lockedStatisticStore->end())
        {
            continue;
        }
        const auto foundStatistic = std::ranges::find_if(
            bucketIt->second,
            [startTs, endTs](const Statistic& statistic) { return statistic.getStartTs() == startTs and statistic.getEndTs() == endTs; });
        if (foundStatistic != bucketIt->second.end())
        {
            return *foundStatistic;
        }
    }
    return {};
}

std::vector<AbstractStatisticStore::IdStatisticPair> SubStoresStatisticStore::getAllStatistics()
{
    std::vector<AbstractStatisticStore::IdStatisticPair> retStatistics;
    for (const auto& statisticStore : allSubStores)
    {
        const auto lockedStatisticStore = statisticStore.rlock();
        for (const auto& [statisticId, bucket] : *lockedStatisticStore)
        {
            for (const auto& statistic : bucket)
            {
                retStatistics.emplace_back(statisticId, statistic);
            }
        }
    }
    return retStatistics;
}
}
