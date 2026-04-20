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
        allSubStores.emplace_back(folly::Synchronized<std::vector<std::shared_ptr<Statistic>>>{});
    }
}

bool SubStoresStatisticStore::insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic)
{
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allSubStores[pos].wlock();
    lockedStatisticStore->emplace_back(std::make_shared<Statistic>(statistic));
    return true;
}

bool SubStoresStatisticStore::deleteStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    const auto lockedStatisticStore = allSubStores[pos].wlock();
    auto newIt = std::ranges::remove_if(
        *lockedStatisticStore,
        [startTs, endTs, statisticId](const std::shared_ptr<Statistic>& statistic)
        { return statisticId == statistic->getStatisticId() and statistic->getStartTs() >= startTs and statistic->getEndTs() <= endTs; });

    const auto foundAnyStatistic = std::ranges::distance(newIt) > 0;
    if (foundAnyStatistic)
    {
        lockedStatisticStore->erase(newIt.begin(), newIt.end());
    }

    return foundAnyStatistic;
}

std::vector<std::shared_ptr<Statistic>> SubStoresStatisticStore::getStatistics(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    std::vector<std::shared_ptr<Statistic>> foundStatistics;
    const auto lockedStatisticStore = allSubStores[pos].rlock();
    std::ranges::copy_if(
        *lockedStatisticStore,
        std::back_inserter(foundStatistics),
        [startTs, endTs, statisticId](const std::shared_ptr<Statistic>& statistic)
        {
            return statistic and statistic->getStatisticId() == statisticId and statistic->getStartTs() >= startTs
                and statistic->getEndTs() <= endTs;
        });
    return foundStatistics;
}

std::optional<std::shared_ptr<Statistic>> SubStoresStatisticStore::getSingleStatistic(
    const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs)
{
    const auto pos = getPos(statisticId, numberOfExpectedConcurrentAccess);
    std::vector<std::shared_ptr<Statistic>> foundStatistics;
    const auto lockedStatisticStore = allSubStores[pos].rlock();
    const auto foundStatistic = std::ranges::find_if(
        *lockedStatisticStore,
        [startTs, endTs, statisticId](const std::shared_ptr<Statistic>& statistic)
        { return statistic->getStatisticId() == statisticId and statistic->getStartTs() == startTs and statistic->getEndTs() == endTs; });
    if (foundStatistic != lockedStatisticStore->end())
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
        auto hashStatisticPairs = *lockedStatisticStore
            | std::views::transform([](const std::shared_ptr<Statistic>& statistic)
                                    { return std::make_pair(statistic->getStatisticId(), statistic); });
        std::ranges::copy(hashStatisticPairs, std::back_inserter(retStatistics));
    }
    return retStatistics;
}
}
