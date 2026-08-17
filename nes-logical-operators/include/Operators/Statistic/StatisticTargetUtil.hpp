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
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramDeltaGenLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramDeltaResolverLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Scalar/ScalarStatisticLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Statistic.hpp>

namespace NES
{

/// Extracts the (statisticId, statisticType) of a statistic aggregation function (CountMinSketch /
/// EquiWidthHistogram / ReservoirSample / the scalar Count / Sum / Avg). Returns nullopt for a non-statistic
/// aggregation. The type is taken from the matched concrete function, so it is the single source of truth
/// (no name-string mapping).
inline std::optional<StatisticTarget> tryGetStatisticTarget(const WindowAggregationLogicalFunction& aggregation)
{
    if (const auto countMin = aggregation.tryGetAs<CountMinSketchLogicalFunction>())
    {
        return StatisticTarget{countMin->get().statisticId, Statistic::StatisticType::Count_Min_Sketch};
    }
    if (const auto histogram = aggregation.tryGetAs<EquiWidthHistogramLogicalFunction>())
    {
        return StatisticTarget{histogram->get().statisticId, Statistic::StatisticType::Equi_Width_Histogram};
    }
    if (const auto histogramDeltaGen = aggregation.tryGetAs<EquiWidthHistogramDeltaGenLogicalFunction>())
    {
        return StatisticTarget{histogramDeltaGen->get().statisticId, Statistic::StatisticType::Equi_Width_Histogram};
    }
    if (const auto histogramDeltaResolver = aggregation.tryGetAs<EquiWidthHistogramDeltaResolverLogicalFunction>())
    {
        return StatisticTarget{histogramDeltaResolver->get().statisticId, Statistic::StatisticType::Equi_Width_Histogram};
    }
    if (const auto sample = aggregation.tryGetAs<ReservoirSampleLogicalFunction>())
    {
        return StatisticTarget{sample->get().statisticId, Statistic::StatisticType::Reservoir_Sample};
    }
    if (const auto scalar = aggregation.tryGetAs<ScalarStatisticLogicalFunction>())
    {
        return StatisticTarget{scalar->get().statisticId, scalar->get().op};
    }
    return std::nullopt;
}

}
