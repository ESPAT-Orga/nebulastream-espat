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

#include <cstdint>
#include <memory>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Identifiers/SketchDimensions.hpp>

namespace NES
{

/// Polymorphic result of StatisticLogicalFunction::calculateConfigs.
/// Each concrete statistic returns a derived struct holding its actual physical parameters.
struct StatisticConfig
{
    virtual ~StatisticConfig() = default;
};

struct ReservoirSampleConfig : StatisticConfig
{
    ReservoirSampleConfig(uint64_t reservoirSize, uint64_t seed) : reservoirSize(reservoirSize), seed(seed) { }

    uint64_t reservoirSize;
    uint64_t seed;
};

struct EquiWidthHistogramConfig : StatisticConfig
{
    EquiWidthHistogramConfig(uint64_t numBuckets, uint64_t minValue, uint64_t maxValue, DataType counterType)
        : numBuckets(numBuckets), minValue(minValue), maxValue(maxValue), counterType(std::move(counterType))
    {
    }

    uint64_t numBuckets;
    uint64_t minValue;
    uint64_t maxValue;
    DataType counterType;
};

struct CountMinSketchConfig : StatisticConfig
{
    CountMinSketchConfig(NumberOfRows rows, NumberOfCols columns, uint64_t seed, DataType counterType)
        : rows(rows), columns(columns), seed(seed), counterType(std::move(counterType))
    {
    }

    NumberOfRows rows;
    NumberOfCols columns;
    uint64_t seed;
    DataType counterType;
};

/// Common base for synopsis logical functions that are sized by a memory budget.
/// Concrete subclasses (ReservoirSample, EquiWidthHistogram, CountMinSketch) override
/// `calculateConfigs` to map the budget onto their concrete parameters during lowering.
class StatisticLogicalFunction
{
public:
    explicit StatisticLogicalFunction(uint64_t memoryBudget);
    virtual ~StatisticLogicalFunction() = default;

    [[nodiscard]] virtual std::unique_ptr<StatisticConfig> calculateConfigs() const = 0;

    uint64_t memoryBudget;
};

}
