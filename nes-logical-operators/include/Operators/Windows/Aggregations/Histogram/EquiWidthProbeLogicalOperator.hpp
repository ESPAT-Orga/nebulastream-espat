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
    explicit EquiWidthProbeLogicalOperator(uint64_t statisticHash, uint64_t minValue, uint64_t maxValue, uint64_t numBuckets);
    explicit EquiWidthProbeLogicalOperator(uint64_t statisticHash, uint64_t minValue, uint64_t maxValue, uint64_t numBuckets, LogicalStatisticFields logicalStatisticFields);

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

        static inline const DescriptorConfig::ConfigParameter<std::string> MIN_VALUE{
            "minValue",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MIN_VALUE, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> MAX_VALUE{
            "maxValue",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_VALUE, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> NUM_BUCKETS{
            "numBuckets",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(NUM_BUCKETS, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(STATISTIC_HASH, MIN_VALUE, MAX_VALUE, NUM_BUCKETS);
    };

    uint64_t statisticHash;
    uint64_t minValue;
    uint64_t maxValue;
    uint64_t numBuckets;

private:
    static constexpr std::string_view NAME = "EquiWidthProbe";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};
}
