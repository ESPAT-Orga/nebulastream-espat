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

class StatisticStoreWriterLogicalOperator final : public LogicalOperatorConcept
{
public:
    StatisticStoreWriterLogicalOperator(
        std::shared_ptr<LogicalStatisticFields> inputLogicalStatisticFields,
        Statistic::StatisticHash statisticHash,
        Statistic::StatisticType statisticType);
    void serialize(SerializableOperator&) const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::vector<struct LogicalOperator> getChildren() const override;
    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator>) const override;
    [[nodiscard]] LogicalOperator withTraitSet(TraitSet) const override;
    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] TraitSet getTraitSet() const override;
    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;
    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>>) const override;
    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId>) const override;
    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;
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

    /// Needed so that the window aggregation build can set the field names
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
