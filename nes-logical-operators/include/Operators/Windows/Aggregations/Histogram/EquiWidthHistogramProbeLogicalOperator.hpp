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
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Builds a new stream by reading out the buckets/bins of the histogram.
class EquiWidthHistogramProbeLogicalOperator final : public LogicalStatisticFields, public OriginIdAssigner
{
public:
    explicit EquiWidthHistogramProbeLogicalOperator(uint64_t statisticHash, DataType counterType, DataType startEndType);
    explicit EquiWidthHistogramProbeLogicalOperator(
        uint64_t statisticHash,
        DataType counterType,
        DataType startEndType,
        std::string binStartFieldName,
        std::string binEndFieldName,
        std::string binCounterFieldName,
        LogicalStatisticFields logicalStatisticFields);

    [[nodiscard]] bool operator==(const EquiWidthHistogramProbeLogicalOperator& rhs) const;

    [[nodiscard]] EquiWidthHistogramProbeLogicalOperator withTraitSet(TraitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] EquiWidthHistogramProbeLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] EquiWidthHistogramProbeLogicalOperator withInferredSchema(const std::vector<Schema>& inputSchemas) const;

    uint64_t statisticHash;
    DataType counterType;
    DataType startEndType;

    std::string binStartFieldName = BIN_START_FIELD_NAME_DEFAULT;
    std::string binEndFieldName = BIN_END_FIELD_NAME_DEFAULT;
    std::string binCounterFieldName = BIN_COUNTER_FIELD_NAME_DEFAULT;

private:
    static constexpr std::string_view NAME = "EquiWidthHistogramProbe";

    static constexpr std::string BIN_START_FIELD_NAME_DEFAULT = "BINSTART";
    static constexpr std::string BIN_END_FIELD_NAME_DEFAULT = "BINEND";
    static constexpr std::string BIN_COUNTER_FIELD_NAME_DEFAULT = "BINCOUNTER";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

template <>
struct Reflector<EquiWidthHistogramProbeLogicalOperator>
{
    Reflected operator()(const EquiWidthHistogramProbeLogicalOperator&) const;
};

template <>
struct Unreflector<EquiWidthHistogramProbeLogicalOperator>
{
    EquiWidthHistogramProbeLogicalOperator operator()(const Reflected&) const;
};

static_assert(LogicalOperatorConcept<EquiWidthHistogramProbeLogicalOperator>);

namespace detail
{
struct ReflectedEquiWidthHistogramProbeLogicalOperator
{
    uint64_t statisticHash;
    DataType counterTypeValue;
    DataType startEndTypeValue;
    std::string binStartFieldName;
    std::string binEndFieldName;
    std::string binCounterFieldName;
};
}

}
