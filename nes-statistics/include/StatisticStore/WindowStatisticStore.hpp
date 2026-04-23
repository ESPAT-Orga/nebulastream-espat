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

/// WindowsStore: Two-level hash index. The outer layer is sharded by StatisticId across
/// numberOfExpectedConcurrentAccess sub-stores; each sub-store maps StatisticId to an
/// inner map keyed by windowStart. This keeps O(1) single-statistic (id, windowStart)
/// lookups (two hash probes on small maps) while letting range queries acquire a single
/// rlock and walk a compact per-id inner map.
/// All windows are of fixed size, set at store initialization.
class WindowStatisticStore final : public AbstractStatisticStore
{
    /// windowStart -> statistics falling into that window
    using WindowMap = std::unordered_map<Windowing::TimeMeasure, std::vector<Statistic>>;
    /// statisticId -> windowMap
    using IdWindowMap = std::unordered_map<Statistic::StatisticId, WindowMap>;

    uint64_t numberOfExpectedConcurrentAccess;
    Windowing::TimeMeasure windowSize;
    std::vector<folly::Synchronized<IdWindowMap>> allStatistics;

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
