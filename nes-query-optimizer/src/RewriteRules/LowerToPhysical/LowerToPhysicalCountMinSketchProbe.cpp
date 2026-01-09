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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalCountMinSketchProbe.hpp>

#include <Operators/Windows/Aggregations/Sketch/CountMinSketchProbeLogicalOperator.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Statistic/Sketch/CountMinSketchIteratorImpl.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticStore/StatisticStoreReader.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

namespace
{
StatisticProvider getStatisticProvider(const DataType counterType, std::string columnFieldName, std::string rowFieldName, std::string counterFieldName)
{
    auto statisticProviderArguments
        = std::make_unique<CountMinSketchProviderArguments>(counterType, columnFieldName, rowFieldName, counterFieldName);
    return {Statistic::StatisticType::Count_Min_Sketch, std::move(statisticProviderArguments)};
}
}

RewriteRuleResultSubgraph LowerToPhysicalCountMinSketchProbe::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<CountMinSketchProbeLogicalOperator>(), "Expected a CountMinSketchProbeLogicalOperator");
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value().memoryLayout;
    const auto countMinProbe = logicalOperator.getAs<CountMinSketchProbeLogicalOperator>();
    auto statisticStore = NodeEngine::getStatisticStore();
    auto statisticStoreReaderOperatorHandler = std::make_shared<StatisticStoreOperatorHandler>(std::move(statisticStore));
    const auto operatorHandlerId = getNextOperatorHandlerId();
    auto statisticProvider = getStatisticProvider(countMinProbe->counterType, countMinProbe->columnIndexFieldName, countMinProbe->rowIndexFieldName, countMinProbe->counterFieldName);
    StatisticStoreReader statisticStoreReader{
        operatorHandlerId,
        countMinProbe->statisticHashField.name,
        countMinProbe->statisticStartTsField.name,
        countMinProbe->statisticEndTsField.name,
        countMinProbe->statisticNumberOfSeenTuplesField.name,
        std::move(statisticProvider)};

    auto inputSchema = countMinProbe.getInputSchemas()[0];
    auto outputSchema = countMinProbe.getOutputSchema();
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(statisticStoreReader),
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        operatorHandlerId,
        statisticStoreReaderOperatorHandler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leaves(countMinProbe.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leaves}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterCountMinProbeRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalCountMinSketchProbe>(argument.conf);
}
}
