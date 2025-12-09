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
#include <fmt/ranges.h>
#include <SerializableOperator.pb.h>
#include "rust/cxx.h"

namespace NES
{
class SerializableOperator;

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
    void serialize(SerializableOperator&) const;

    [[nodiscard]] EquiWidthHistogramProbeLogicalOperator withTraitSet(TraitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] EquiWidthHistogramProbeLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] EquiWidthHistogramProbeLogicalOperator withInferredSchema(const std::vector<Schema>& inputSchemas) const;

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

        static inline const DescriptorConfig::ConfigParameter<std::string> START_END_TYPE{
            "startEndType",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(START_END_TYPE, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> BIN_START_FIELD_NAME{
            "binStartFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BIN_START_FIELD_NAME, config); }};
        static inline const DescriptorConfig::ConfigParameter<std::string> BIN_END_FIELD_NAME{
            "binEndFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BIN_END_FIELD_NAME, config); }};
        static inline const DescriptorConfig::ConfigParameter<std::string> BIN_COUNTER_FIELD_NAME{
            "binCounterFieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BIN_COUNTER_FIELD_NAME, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(
                STATISTIC_HASH, COUNTER_TYPE, START_END_TYPE, BIN_START_FIELD_NAME, BIN_END_FIELD_NAME, BIN_COUNTER_FIELD_NAME);
    };

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
}
