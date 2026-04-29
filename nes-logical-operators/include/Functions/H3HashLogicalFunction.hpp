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
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
/// SQL-callable companion to CountMinSketch's H3 hash. Given a value and a row
/// of the sketch, returns the column index that the build pipeline would have
/// targeted for that value at that row. Lets accuracy queries express CountMin
/// point/range estimates on top of the raw COUNTMIN_PROBE output (which dumps
/// the sketch but doesn't expose the hash).
///
/// Signature: H3_HASH(value, rowIndex, numberOfRows, numberOfCols, seed) -> UINT64
///   value         arbitrary numeric value to hash; treated as uint64 bit pattern
///   rowIndex      0-based sketch row (matches CountMin probe output's rowIndex)
///   numberOfRows  total rows of the sketch (per-statistic constant)
///   numberOfCols  total columns of the sketch (per-statistic constant)
///   seed          mt19937 seed used by the build pipeline (kCountMinSeed = 42)
class H3HashLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "H3_HASH";

    H3HashLogicalFunction(
        const LogicalFunction& value,
        const LogicalFunction& rowIndex,
        const LogicalFunction& numberOfRows,
        const LogicalFunction& numberOfCols,
        const LogicalFunction& seed);

    [[nodiscard]] bool operator==(const H3HashLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] H3HashLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] H3HashLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction value;
    LogicalFunction rowIndex;
    LogicalFunction numberOfRows;
    LogicalFunction numberOfCols;
    LogicalFunction seed;
    friend Reflector<H3HashLogicalFunction>;
};

template <>
struct Reflector<H3HashLogicalFunction>
{
    Reflected operator()(const H3HashLogicalFunction& function) const;
};

template <>
struct Unreflector<H3HashLogicalFunction>
{
    H3HashLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<H3HashLogicalFunction>);
}

namespace NES::detail
{
struct ReflectedH3HashLogicalFunction
{
    std::optional<LogicalFunction> value;
    std::optional<LogicalFunction> rowIndex;
    std::optional<LogicalFunction> numberOfRows;
    std::optional<LogicalFunction> numberOfCols;
    std::optional<LogicalFunction> seed;
};
}

FMT_OSTREAM(NES::H3HashLogicalFunction);
