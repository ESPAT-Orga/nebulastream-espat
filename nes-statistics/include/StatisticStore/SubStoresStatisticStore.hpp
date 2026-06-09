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

#pragma once

#include <map>
#include <unordered_map>

#include <StatisticStore/AbstractStatisticStore.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <folly/Synchronized.h>

namespace NES
{

/// Thread-sharded statistic store: each worker thread always writes to the same shard
/// (determined by hashing std::this_thread::get_id()), so inserts from different threads
/// never contend. Reads must scan all shards, but each shard's inner storage is now an
/// ordered map keyed by startTs so that getStatistics range queries run in O(log N + k)
/// per shard rather than O(N), where N is the number of stored windows per statistic ID
/// in that shard and k is the number of matches.
class SubStoresStatisticStore final : public AbstractStatisticStore
{
    /// startTs → statistics sharing that startTs
    using WindowMap = std::map<Windowing::TimeMeasure, std::vector<Statistic>>;
    /// statisticId → windowMap
    using IdWindowMap = std::unordered_map<Statistic::StatisticId, WindowMap>;

    uint64_t numberOfExpectedConcurrentAccess;
    std::vector<folly::Synchronized<IdWindowMap>> allSubStores;

public:
    explicit SubStoresStatisticStore(uint64_t numberOfExpectedConcurrentAccess);
    bool insertStatistic(const Statistic::StatisticId& statisticId, Statistic statistic) override;
    bool deleteStatistics(
        const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::vector<Statistic> getStatistics(
        const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::optional<Statistic> getSingleStatistic(
        const Statistic::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::vector<IdStatisticPair> getAllStatistics() override;
};

}
