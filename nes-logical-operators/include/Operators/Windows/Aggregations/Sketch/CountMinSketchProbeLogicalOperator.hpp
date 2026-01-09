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
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>
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

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_HASH{
            "statisticHash",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(STATISTIC_HASH, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> COUNTER_TYPE{
            "counterType",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(COUNTER_TYPE, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> ROW_INDEX_FIELD_NAME{
            "rowIndexFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(ROW_INDEX_FIELD_NAME, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> COLUMN_INDEX_FIELD_NAME{
            "columnIndexFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(COLUMN_INDEX_FIELD_NAME, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> COUNTER_FIELD_NAME{
            "counterFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(COUNTER_FIELD_NAME, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(
                STATISTIC_HASH, COUNTER_TYPE, ROW_INDEX_FIELD_NAME, COLUMN_INDEX_FIELD_NAME, COUNTER_FIELD_NAME);
    };

    Statistic::StatisticHash statisticHash;
    DataType counterType;

    std::string rowIndexFieldName = ROW_INDEX_FIELD_NAME_DEFAULT;
    std::string columnIndexFieldName = COLUMN_INDEX_FIELD_NAME_DEFAULT;
    std::string counterFieldName = COUNTER_FIELD_NAME_DEFAULT;

    static constexpr DataType indexType = DataType{DataType::Type::UINT64};

private:
    static constexpr std::string_view NAME = "CountMinProbe";

    static constexpr std::string ROW_INDEX_FIELD_NAME_DEFAULT = "ROWINDEX";
    static constexpr std::string COLUMN_INDEX_FIELD_NAME_DEFAULT = "COLUMNINDEX";
    static constexpr std::string COUNTER_FIELD_NAME_DEFAULT = "COUNTER";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};
}
