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

namespace NES
{
class SerializableOperator;

/// Builds a new stream by reading tuples from the sample.
class EquiWidthProbeLogicalOperator final : public LogicalStatisticFields, public OriginIdAssigner
{
public:
    explicit EquiWidthProbeLogicalOperator(uint64_t statisticHash, DataType counterType, DataType startEndType);
    explicit EquiWidthProbeLogicalOperator(uint64_t statisticHash, DataType counterType, DataType startEndType, LogicalStatisticFields logicalStatisticFields);

    [[nodiscard]] bool operator==(const EquiWidthProbeLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] EquiWidthProbeLogicalOperator withTraitSet(TraitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] EquiWidthProbeLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] EquiWidthProbeLogicalOperator withInferredSchema(const std::vector<Schema>& inputSchemas) const;

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

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(STATISTIC_HASH, COUNTER_TYPE, START_END_TYPE);
    };

    uint64_t statisticHash;
    DataType counterType;
    DataType startEndType;

private:
    static constexpr std::string_view NAME = "EquiWidthProbe";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};
}
