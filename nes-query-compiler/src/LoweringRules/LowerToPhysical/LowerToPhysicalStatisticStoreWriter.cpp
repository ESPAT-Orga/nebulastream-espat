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
#include <LoweringRules/LowerToPhysical/LowerToPhysicalStatisticStoreWriter.hpp>

#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticStore/StatisticStoreWriter.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>

namespace NES
{

LoweringRuleResultSubgraph
LowerToPhysicalStatisticStoreWriter::apply(LogicalOperator logicalOperator, const std::shared_ptr<AbstractStatisticStore>& statisticStore)
{
    PRECONDITION(logicalOperator.tryGetAs<StatisticStoreWriterLogicalOperator>(), "Expected a StatisticStoreWriterLogicalOperator");
    const auto logicalStatisticStoreWriter = logicalOperator.getAs<StatisticStoreWriterLogicalOperator>();
    auto inputSchema = logicalStatisticStoreWriter.getInputSchemas()[0];
    auto statisticStoreWriterOperatorHandler = std::make_shared<StatisticStoreOperatorHandler>(statisticStore);
    const auto operatorHandlerId = getNextOperatorHandlerId();
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    StatisticStoreWriter statisticStoreWriter{
        operatorHandlerId,
        logicalStatisticStoreWriter->getStatisticId(),
        logicalStatisticStoreWriter->getStatisticType(),
        *logicalStatisticStoreWriter->inputLogicalStatisticFields,
        logicalStatisticStoreWriter->getOutputStatisticFields(inputSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator())};


    auto outputSchema = logicalStatisticStoreWriter.getOutputSchema();
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        statisticStoreWriter,
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        operatorHandlerId,
        statisticStoreWriterOperatorHandler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leafs(logicalStatisticStoreWriter.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafs}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterStatisticStoreWriterLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalStatisticStoreWriter>(argument.conf);
}
}
