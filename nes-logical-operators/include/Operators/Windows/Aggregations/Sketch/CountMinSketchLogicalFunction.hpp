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

/// The CountMinSketch estimates counts of `asField` values, where accuracy depends on `columns` and `rows`.
class CountMinSketchLogicalFunction final : public WindowAggregationLogicalFunction
{
public:
    /// `onField` the field for which the sketch should be created
    /// `asField` used when the sketch should be renamed in the query
    /// `columns` how many possible "buckets" per hashfunction
    /// `rows` equal to number of hash functions used
    CountMinSketchLogicalFunction(const FieldAccessLogicalFunction& onField, uint64_t columns, uint64_t rows);
    CountMinSketchLogicalFunction(
        const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField, uint64_t columns, uint64_t rows);

    void inferStamp(const Schema& schema) override;
    ~CountMinSketchLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    uint64_t columns;
    uint64_t rows;

private:
    static constexpr std::string_view NAME = "CountMinSketch";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;
};

}
