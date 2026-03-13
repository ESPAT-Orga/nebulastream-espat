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
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// The EquiWidthHistogram will count the values of the `asField` in the range of `minValue` and `maxValue`. The range is
/// partitioned into `numBuckets` equi-width buckets.
class EquiWidthHistogramLogicalFunction
{
public:
    /// `asField` used when the histogram should be renamed in the query
    /// `numBuckets` number of buckets the histogram should have.
    /// `minValue` start value of the histogram
    /// `maxValue` end value of the histogram
    /// `statisticHash` the number that identifies this synopsis in the statistic store to later retrieve it
    /// `counterType` data type of the counter in each bucket
    EquiWidthHistogramLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        uint64_t numBuckets,
        uint64_t minValue,
        uint64_t maxValue,
        uint64_t statisticHash,
        DataType counterType);
    EquiWidthHistogramLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        const FieldAccessLogicalFunction& asField,
        uint64_t numBuckets,
        uint64_t minValue,
        uint64_t maxValue,
        uint64_t statisticHash,
        DataType counterType);

    ~EquiWidthHistogramLogicalFunction() = default;

    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] EquiWidthHistogramLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] EquiWidthHistogramLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] EquiWidthHistogramLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] EquiWidthHistogramLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] EquiWidthHistogramLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] EquiWidthHistogramLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;

    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const EquiWidthHistogramLogicalFunction& rhs) const;

    uint64_t numBuckets;
    uint64_t minValue;
    uint64_t maxValue;

    uint64_t statisticHash;
    DataType counterType;

private:
    static constexpr std::string_view NAME = "EquiWidthHistogram";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<EquiWidthHistogramLogicalFunction>);

template <>
struct Reflector<EquiWidthHistogramLogicalFunction>
{
    Reflected operator()(const EquiWidthHistogramLogicalFunction& function) const;
};

template <>
struct Unreflector<EquiWidthHistogramLogicalFunction>
{
    EquiWidthHistogramLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedEquiWidthHistogramLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    uint64_t numBuckets;
    uint64_t minValue;
    uint64_t maxValue;
    uint64_t statisticHash;
    DataType counterType;
};
}
