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
#include <Identifiers/SketchDimensions.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <Statistic.hpp>

namespace NES
{

/// The CountMinSketch estimates counts of `asField` values, where accuracy depends on `columns` and `rows`.
/// The actual errors can be taken from the paper: An Improved Data Stream Summary: The Count-Min Sketch and its Applications
/// https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
class CountMinSketchLogicalFunction
{
public:
    CountMinSketchLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        NumberOfCols columns,
        NumberOfRows rows,
        uint64_t seed,
        DataType counterType,
        Statistic::StatisticHash statisticHash);
    /// `onField` the field for which the sketch should be created
    /// `asField` used when the sketch should be renamed in the query
    /// `columns` how many possible "buckets" per hashfunction
    /// `rows` equal to number of hash functions used
    /// `seed` used to generate seeds to differentiate the hash functions between `rows`
    /// `counterType` datatype of each entry in the `column`x`row` matrix
    CountMinSketchLogicalFunction(
        const FieldAccessLogicalFunction& onField,
        const FieldAccessLogicalFunction& asField,
        NumberOfCols columns,
        NumberOfRows rows,
        uint64_t seed,
        DataType counterType,
        Statistic::StatisticHash statisticHash);

    ~CountMinSketchLogicalFunction() = default;

    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] CountMinSketchLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] CountMinSketchLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] CountMinSketchLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] CountMinSketchLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] CountMinSketchLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] CountMinSketchLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;

    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const CountMinSketchLogicalFunction& rhs) const;

    NumberOfCols columns;
    NumberOfRows rows;
    uint64_t seed;

    DataType counterType;
    Statistic::StatisticHash statisticHash;

private:
    static constexpr std::string_view NAME = "CountMinSketch";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<CountMinSketchLogicalFunction>);

template <>
struct Reflector<CountMinSketchLogicalFunction>
{
    Reflected operator()(const CountMinSketchLogicalFunction& function) const;
};

template <>
struct Unreflector<CountMinSketchLogicalFunction>
{
    CountMinSketchLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedCountMinSketchLogicalFunction
{
    Statistic::StatisticHash statisticHash;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    uint64_t columns;
    uint64_t rows;
    uint64_t seed;
    DataType counterType;
};
}
