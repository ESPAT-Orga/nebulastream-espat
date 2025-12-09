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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalEquiWidthHistogramProbe.hpp>

#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramProbeLogicalOperator.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Statistic/Histogram/EquiWidthHistogramIteratorImpl.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticStore/StatisticStoreReader.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

namespace
{
StatisticProvider getStatisticProvider(
    const DataType counterType,
    const DataType startEndType,
    const std::string& binStartFieldName,
    const std::string& binEndFieldName,
    const std::string& binCounterFieldName)
{
    auto statisticProviderArguments = std::make_unique<EquiWidthHistogramProviderArguments>(
        binStartFieldName, binEndFieldName, binCounterFieldName, counterType, startEndType);
    return {Statistic::StatisticType::Equi_Width_Histogram, std::move(statisticProviderArguments)};
}
}

RewriteRuleResultSubgraph LowerToPhysicalEquiWidthHistogramProbe::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<EquiWidthHistogramProbeLogicalOperator>(), "Expected a EquiWidthHistogramProbeLogicalOperator");
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value().memoryLayout;
    const auto equiWidthProbe = logicalOperator.getAs<EquiWidthHistogramProbeLogicalOperator>();
    auto statisticStore = NodeEngine::getStatisticStore();
    auto statisticStoreReaderOperatorHandler = std::make_shared<StatisticStoreOperatorHandler>(std::move(statisticStore));
    const auto operatorHandlerId = getNextOperatorHandlerId();
    auto statisticProvider = getStatisticProvider(
        equiWidthProbe->counterType,
        equiWidthProbe->startEndType,
        equiWidthProbe->binStartFieldName,
        equiWidthProbe->binEndFieldName,
        equiWidthProbe->binCounterFieldName);
    StatisticStoreReader statisticStoreReader{
        operatorHandlerId,
        equiWidthProbe->statisticHashField.name,
        equiWidthProbe->statisticStartTsField.name,
        equiWidthProbe->statisticEndTsField.name,
        equiWidthProbe->statisticNumberOfSeenTuplesField.name,
        std::move(statisticProvider)};

    auto inputSchema = equiWidthProbe.getInputSchemas()[0];
    auto outputSchema = equiWidthProbe.getOutputSchema();
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
    const std::vector leafs(equiWidthProbe.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafs}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterEquiWidthHistogramProbeRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalEquiWidthHistogramProbe>(argument.conf);
}
}
