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

#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Statistic.hpp>

namespace NES
{
class SerializableOperator;
class SerializableSchema_SerializableField;

/// Logical operator that writes a statistic to the statistic store on the node.
/// This operator decides what node stores the built statistic emitted from the statistic query.
class StatisticStoreWriterLogicalOperator final
{
public:
    StatisticStoreWriterLogicalOperator(
        std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
        Statistic::StatisticHash statisticHash,
        Statistic::StatisticType statisticType);
    void serialize(SerializableOperator&) const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] StatisticStoreWriterLogicalOperator withChildren(std::vector<LogicalOperator>) const;
    [[nodiscard]] StatisticStoreWriterLogicalOperator withTraitSet(TraitSet) const;
    [[nodiscard]] bool operator==(const StatisticStoreWriterLogicalOperator& rhs) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;
    [[nodiscard]] StatisticStoreWriterLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;
    [[nodiscard]] Statistic::StatisticHash getStatisticHash() const;
    [[nodiscard]] Statistic::StatisticType getStatisticType() const;
    [[nodiscard]] static LogicalStatisticFields getOutputStatisticFields(const std::string_view qualifierName);

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_HASH_VALUE{
            "statisticHashValue",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(STATISTIC_HASH_VALUE, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> STATISTIC_TYPE_VALUE{
            "statisticTypeValue",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(STATISTIC_TYPE_VALUE, config); }};
    };

    /// Needs to be shared so that the window aggregation build can set the field names
    std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields;

private:
    static constexpr std::string_view NAME = "StatisticStoreWriter";

    Statistic::StatisticHash statisticHash;
    Statistic::StatisticType statisticType;
    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

}
