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
#include <RewriteRules/LowerToPhysical/LowerToPhysicalStatisticStoreWriter.hpp>

#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticStore/StatisticStoreWriter.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalStatisticStoreWriter::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<StatisticStoreWriterLogicalOperator>(), "Expected a StatisticStoreWriterLogicalOperator");
    const auto logicalStatisticStoreWriter = logicalOperator.getAs<StatisticStoreWriterLogicalOperator>();
    auto inputSchema = logicalStatisticStoreWriter.getInputSchemas()[0];
    auto statisticStore = NodeEngine::getStatisticStore();
    auto statisticStoreWriterOperatorHandler = std::make_shared<StatisticStoreOperatorHandler>(std::move(statisticStore));
    const auto operatorHandlerId = getNextOperatorHandlerId();
    StatisticStoreWriter statisticStoreWriter{
        operatorHandlerId,
        logicalStatisticStoreWriter->getStatisticHash(),
        logicalStatisticStoreWriter->getStatisticType(),
        *logicalStatisticStoreWriter->inputLogicalStatisticFields,
        logicalStatisticStoreWriter->getOutputStatisticFields(inputSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator())};


    auto outputSchema = logicalStatisticStoreWriter.getOutputSchema();
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        statisticStoreWriter,
        inputSchema,
        outputSchema,
        operatorHandlerId,
        statisticStoreWriterOperatorHandler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leafs(logicalStatisticStoreWriter.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafs}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterStatisticStoreWriterRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalStatisticStoreWriter>(argument.conf);
}
}
