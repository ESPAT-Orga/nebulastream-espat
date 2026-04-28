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

#include <functional>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <folly/Synchronized.h>
#include <Statistic.hpp>

namespace NES
{

/// WindowsStore: Maps each (key, window_start_time) pair to a vector of statistics.
/// All windows are of fixed size, set at store initialization, enabling time-based partitioning of statistics per key.
class WindowStatisticStore final : public AbstractStatisticStore
{
    struct StatisticKey
    {
        Statistic::StatisticId statisticId;
        Windowing::TimeMeasure startTs;

        bool operator==(const StatisticKey& other) const
        {
            return this->statisticId == other.statisticId && this->startTs == other.startTs;
        }
    };

    struct StatisticKeyHash
    {
        size_t operator()(const StatisticKey& key) const
        {
            const auto h1 = std::hash<Statistic::StatisticId>{}(key.statisticId);
            const auto h2 = std::hash<Windowing::TimeMeasure>{}(key.startTs);
            return h1 ^ (h2 << 1);
        }
    };

    uint64_t numberOfExpectedConcurrentAccess;
    Windowing::TimeMeasure windowSize;
    std::vector<folly::Synchronized<std::unordered_map<StatisticKey, std::vector<Statistic>, StatisticKeyHash>>> allStatistics;

    Windowing::TimeMeasure calculateWindowStartTime(Windowing::TimeMeasure statStartTime) const;

public:
    explicit WindowStatisticStore(uint64_t numberOfExpectedConcurrentAccess, Windowing::TimeMeasure windowSize);
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
