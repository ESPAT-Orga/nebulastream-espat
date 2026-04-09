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
#include <iterator>
#include <ranges>
#include <unordered_map>
#include <utility>

namespace NES
{
namespace
{
uint64_t getPos(const Statistic::StatisticId& statisticId, const uint64_t numberOfExpectedConcurrentAccess)
{
    /// We use here the randomness of the statisticId to distribute the accesses.
    /// We can not use a worker thread id or etc, as this function is not only called from the execution
    const auto pos = statisticId % numberOfExpectedConcurrentAccess;
    return pos;
}
}

SubStoresStatisticStore::SubStoresStatisticStore(const uint64_t numberOfExpectedConcurrentAccess)
    : numberOfExpectedConcurrentAccess(numberOfExpectedConcurrentAccess)
{
    allSubStores.reserve(numberOfExpectedConcurrentAccess);
    for (uint64_t i = 0; i < numberOfExpectedConcurrentAccess; ++i)
    {
        allSubStores.emplace_back(
            folly::Synchronized<std::unordered_map<Statistic::StatisticId, std::vector<std::shared_ptr<Statistic>>>>{});
    }
}

bool SubStoresStatisticStore::insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic)
{
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allSubStores[pos].wlock();
    (*lockedStatisticStore)[statisticId].emplace_back(std::make_shared<Statistic>(statistic));
    return true;
}

bool SubStoresStatisticStore::deleteStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allSubStores[pos].wlock();
    const auto it = lockedStatisticStore->find(statisticId);
    if (it == lockedStatisticStore->end())
    {
        return false;
    }

    auto& bucket = it->second;
    auto newEnd = std::ranges::remove_if(
        bucket,
        [startTs, endTs](const std::shared_ptr<Statistic>& statistic)
        { return statistic->getStartTs() >= startTs and statistic->getEndTs() <= endTs; });

    const auto foundAnyStatistic = newEnd.begin() != bucket.end();
    if (foundAnyStatistic)
    {
        bucket.erase(newEnd.begin(), bucket.end());
    }

    return foundAnyStatistic;
}

std::vector<std::shared_ptr<Statistic>> SubStoresStatisticStore::getStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    std::vector<std::shared_ptr<Statistic>> foundStatistics;
    const auto lockedStatisticStore = allSubStores[pos].rlock();
    const auto it = lockedStatisticStore->find(statisticId);
    if (it == lockedStatisticStore->end())
    {
        return foundStatistics;
    }

    std::ranges::copy_if(
        it->second,
        std::back_inserter(foundStatistics),
        [startTs, endTs](const std::shared_ptr<Statistic>& statistic)
        { return statistic->getStartTs() >= startTs and statistic->getEndTs() <= endTs; });
    return foundStatistics;
}

std::optional<std::shared_ptr<Statistic>> SubStoresStatisticStore::getSingleStatistic(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allSubStores[pos].rlock();
    const auto it = lockedStatisticStore->find(statisticId);
    if (it == lockedStatisticStore->end())
    {
        return {};
    }

    const auto foundStatistic = std::ranges::find_if(
        it->second,
        [startTs, endTs](const std::shared_ptr<Statistic>& statistic)
        { return statistic->getStartTs() == startTs and statistic->getEndTs() == endTs; });
    if (foundStatistic != it->second.end())
    {
        return *foundStatistic;
    }
    return {};
}

std::vector<AbstractStatisticStore::IdStatisticPair> SubStoresStatisticStore::getAllStatistics()
{
    std::vector<AbstractStatisticStore::IdStatisticPair> retStatistics;
    for (const auto& statisticStore : allSubStores)
    {
        const auto lockedStatisticStore = statisticStore.rlock();
        for (const auto& [id, bucket] : *lockedStatisticStore)
        {
            auto pairs = bucket
                | std::views::transform([&id](const std::shared_ptr<Statistic>& statistic)
                                        { return std::make_pair(id, statistic); });
            std::ranges::copy(pairs, std::back_inserter(retStatistics));
        }
    }
    return retStatistics;
}
}
