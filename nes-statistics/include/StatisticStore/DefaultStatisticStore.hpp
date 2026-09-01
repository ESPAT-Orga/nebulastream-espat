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

#include <optional>
#include <unordered_map>
#include <vector>
#include <StatisticTuple.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <folly/Synchronized.h>

namespace NES
{

/// The simplest store: one lock over a map from statistic id to an insertion-ordered vector of statistics.
/// Range queries are a linear scan, which is fine at the volumes a single statistic interface probes.
class DefaultStatisticStore final : public AbstractStatisticStore
{
public:
    bool insertStatistic(const StatisticTuple::StatisticId& statisticId, StatisticTuple statistic) override;
    bool deleteStatistics(
        const StatisticTuple::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::vector<StatisticTuple> getStatistics(
        const StatisticTuple::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::optional<StatisticTuple> getSingleStatistic(
        const StatisticTuple::StatisticId& statisticId, const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs) override;
    std::vector<IdStatisticPair> getAllStatistics() override;

private:
    folly::Synchronized<std::unordered_map<StatisticTuple::StatisticId, std::vector<StatisticTuple>>> statistics;
};

}
