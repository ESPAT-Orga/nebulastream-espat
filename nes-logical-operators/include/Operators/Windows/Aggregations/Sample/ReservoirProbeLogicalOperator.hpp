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
#include <Statistic.hpp>

namespace NES
{

/// Builds a new stream by reading tuples from the sample.
class ReservoirProbeLogicalOperator final : public LogicalStatisticFields, public OriginIdAssigner
{
public:
    explicit ReservoirProbeLogicalOperator(Statistic::StatisticId statisticId, const Schema& sampleSchema);
    explicit ReservoirProbeLogicalOperator(
        Statistic::StatisticId statisticId, const Schema& sampleSchema, LogicalStatisticFields logicalStatisticFields);

    [[nodiscard]] bool operator==(const ReservoirProbeLogicalOperator& rhs) const;

    [[nodiscard]] ReservoirProbeLogicalOperator withTraitSet(TraitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] ReservoirProbeLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] ReservoirProbeLogicalOperator withInferredSchema(const std::vector<Schema>& inputSchemas) const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_ID{
            "statisticId",
            {},
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(STATISTIC_ID, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> SAMPLE_SCHEMA{
            "sampleSchema",
            {},
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SAMPLE_SCHEMA, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(STATISTIC_ID, SAMPLE_SCHEMA);
    };

    /// Name of the field the sample is in.
    Statistic::StatisticId statisticId;
    Schema sampleSchema;

private:
    static constexpr std::string_view NAME = "ReservoirProbe";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

template <>
struct Reflector<ReservoirProbeLogicalOperator>
{
    Reflected operator()(const ReservoirProbeLogicalOperator&) const;
};

template <>
struct Unreflector<ReservoirProbeLogicalOperator>
{
    ReservoirProbeLogicalOperator operator()(const Reflected&) const;
};

static_assert(LogicalOperatorConcept<ReservoirProbeLogicalOperator>);

namespace detail
{
struct ReflectedSchemaField
{
    std::string name;
    uint8_t dataType;
};

struct ReflectedReservoirProbeLogicalOperator
{
    Statistic::StatisticId::Underlying statisticId;
    std::vector<ReflectedSchemaField> sampleFields;
};
}

}
