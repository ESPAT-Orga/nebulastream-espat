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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalEquiWidthProbe.hpp>

#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthProbeLogicalOperator.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Statistic/Histogram/EquiWidthHistogramIteratorImpl.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticStore/StatisticStoreReader.hpp>
#include <ErrorHandling.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

namespace
{
StatisticProvider getStatisticProvider(const Schema& sampleSchema)
{
    std::unique_ptr<MemoryLayout> memoryLayout;
    switch (sampleSchema.memoryLayoutType)
    {
        case Schema::MemoryLayoutType::ROW_LAYOUT:
            /// For a row layout, we do not care about the overall memory area size
            memoryLayout = std::make_unique<RowLayout>(0, sampleSchema);
            break;
        case Schema::MemoryLayoutType::COLUMNAR_LAYOUT:
            throw NotImplemented(
                "TODO We _do_ know the size of the Histogram, so this should work?");
    }

    auto statisticProviderArguments = std::make_unique<EquiWidthHistogramProviderArguments>(std::move(memoryLayout));
    return {Statistic::StatisticType::Equi_Width_Histogram, std::move(statisticProviderArguments)};
}
}

RewriteRuleResultSubgraph LowerToPhysicalEquiWidthProbe::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<EquiWidthProbeLogicalOperator>(), "Expected a EquiWidthProbeLogicalOperator");
    const auto equiWidthProbe = logicalOperator.getAs<EquiWidthProbeLogicalOperator>();
    auto statisticStore = NodeEngine::getStatisticStore();
    auto statisticStoreReaderOperatorHandler = std::make_shared<StatisticStoreOperatorHandler>(std::move(statisticStore));
    const auto operatorHandlerId = getNextOperatorHandlerId();
    auto statisticProvider = getStatisticProvider(equiWidthProbe->getSchema());
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
        operatorHandlerId,
        statisticStoreReaderOperatorHandler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leafs(equiWidthProbe.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafs}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterEquiWidthProbeRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalEquiWidthProbe>(argument.conf);
}
}
