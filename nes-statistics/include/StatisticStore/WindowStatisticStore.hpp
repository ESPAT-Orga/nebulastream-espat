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
#include <map>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <folly/Synchronized.h>
#include <Statistic.hpp>

namespace NES
{

/// WindowsStore: Two-level index sharded by StatisticId across numberOfExpectedConcurrentAccess
/// sub-stores (hash outer); each sub-store maps StatisticId to an inner ordered map keyed by the
/// statistic's actual startTs. Point lookups are O(log N) (tree find) and range queries are
/// O(log N + k) via lower_bound/upper_bound, so no fixed window size needs to be known upfront.
class WindowStatisticStore final : public AbstractStatisticStore
{
    /// startTs -> statistics sharing that startTs
    using WindowMap = std::map<Windowing::TimeMeasure, std::vector<Statistic>>;
    /// statisticId -> windowMap
    using IdWindowMap = std::unordered_map<Statistic::StatisticId, WindowMap>;

    uint64_t numberOfExpectedConcurrentAccess;
    std::vector<folly::Synchronized<IdWindowMap>> allStatistics;

public:
    explicit WindowStatisticStore(uint64_t numberOfExpectedConcurrentAccess);
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
