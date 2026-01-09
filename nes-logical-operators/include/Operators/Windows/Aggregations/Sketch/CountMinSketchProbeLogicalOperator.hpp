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
#include <Statistic.hpp>

namespace NES
{
class SerializableOperator;

/// Builds a new stream by reading out all the counters of the Count Min sketch.
class CountMinSketchProbeLogicalOperator final : public LogicalStatisticFields, public OriginIdAssigner
{
public:
    explicit CountMinSketchProbeLogicalOperator(Statistic::StatisticHash statisticHash, DataType counterType);
    explicit CountMinSketchProbeLogicalOperator(
        Statistic::StatisticHash statisticHash,
        DataType counterType,
        std::string rowIndexFieldName,
        std::string columnIndexFieldName,
        std::string counterFieldName,
        LogicalStatisticFields logicalStatisticFields);

    [[nodiscard]] bool operator==(const CountMinSketchProbeLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] CountMinSketchProbeLogicalOperator withTraitSet(TraitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] CountMinSketchProbeLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] CountMinSketchProbeLogicalOperator withInferredSchema(const std::vector<Schema>& inputSchemas) const;

    Statistic::StatisticHash statisticHash;
    DataType counterType;

    std::string rowIndexFieldName = ROW_INDEX_FIELD_NAME_DEFAULT;
    std::string columnIndexFieldName = COLUMN_INDEX_FIELD_NAME_DEFAULT;
    std::string counterFieldName = COUNTER_FIELD_NAME_DEFAULT;

    const DataType indexType = DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};

private:
    static constexpr std::string_view NAME = "CountMinSketchProbe";

    static constexpr std::string ROW_INDEX_FIELD_NAME_DEFAULT = "ROWINDEX";
    static constexpr std::string COLUMN_INDEX_FIELD_NAME_DEFAULT = "COLUMNINDEX";
    static constexpr std::string COUNTER_FIELD_NAME_DEFAULT = "COUNTER";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

template <>
struct Reflector<CountMinSketchProbeLogicalOperator>
{
    Reflected operator()(const CountMinSketchProbeLogicalOperator&) const;
};

template <>
struct Unreflector<CountMinSketchProbeLogicalOperator>
{
    CountMinSketchProbeLogicalOperator operator()(const Reflected&) const;
};

static_assert(LogicalOperatorConcept<CountMinSketchProbeLogicalOperator>);

namespace detail
{
struct ReflectedCountMinSketchProbeLogicalOperator
{
    uint64_t statisticHash;
    DataType counterType;
    std::string rowIndexFieldName;
    std::string columnIndexFieldName;
    std::string counterFieldName;
};
}

}
