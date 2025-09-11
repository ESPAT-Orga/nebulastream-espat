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
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES
{

/// The EquiWidthHistogram will count the values of the `asField` in the range of `minValue` and `maxValue`. The range is
/// partitioned into `numBuckets` equi width buckets.
class EquiWidthHistogramLogicalFunction final : public WindowAggregationLogicalFunction
{
public:
    /// `asField` used when the histogram should be renamed in the query
    /// `numBuckets` number of buckets the histogram should have.
    /// `minValue` start value of the histogram
    /// `maxValue` end value of the histogram
    EquiWidthHistogramLogicalFunction(const FieldAccessLogicalFunction& onField, uint64_t numBuckets, uint64_t minValue, uint64_t maxValue);
    EquiWidthHistogramLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        const FieldAccessLogicalFunction& asField,
        uint64_t numBuckets,
        uint64_t minValue,
        uint64_t maxValue);

    void inferStamp(const Schema& schema) override;
    ~EquiWidthHistogramLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    uint64_t numBuckets;
    uint64_t minValue;
    uint64_t maxValue;

private:
    static constexpr std::string_view NAME = "EquiWidthHistogram";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;
};

}
