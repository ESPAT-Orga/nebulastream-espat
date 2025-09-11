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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalReservoirProbe.hpp>

#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirProbeLogicalOperator.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Statistic/Sample/ReservoirSampleIteratorImpl.hpp>
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
                "Not possible currently to use a columnar layout, as we do not know the size of the sample at this moment.");
    }

    auto statisticProviderArguments = std::make_unique<ReservoirSampleProviderArguments>(std::move(memoryLayout));
    return {Statistic::StatisticType::Reservoir_Sample, std::move(statisticProviderArguments)};
}
}

RewriteRuleResultSubgraph LowerToPhysicalReservoirProbe::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<ReservoirProbeLogicalOperator>(), "Expected a ReservoirProbeLogicalOperator");
    const auto reservoirProbe = logicalOperator.getAs<ReservoirProbeLogicalOperator>();
    auto statisticStore = NodeEngine::getStatisticStore();
    auto statisticStoreReaderOperatorHandler = std::make_shared<StatisticStoreOperatorHandler>(std::move(statisticStore));
    const auto operatorHandlerId = getNextOperatorHandlerId();
    auto statisticProvider = getStatisticProvider(reservoirProbe->sampleSchema);
    StatisticStoreReader statisticStoreReader{
        operatorHandlerId,
        reservoirProbe->statisticHashField.name,
        reservoirProbe->statisticStartTsField.name,
        reservoirProbe->statisticEndTsField.name,
        reservoirProbe->statisticNumberOfSeenTuplesField.name,
        std::move(statisticProvider)};

    auto inputSchema = reservoirProbe.getInputSchemas()[0];
    auto outputSchema = reservoirProbe.getOutputSchema();
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(statisticStoreReader),
        inputSchema,
        outputSchema,
        operatorHandlerId,
        statisticStoreReaderOperatorHandler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leafs(reservoirProbe.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafs}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterReservoirProbeRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalReservoirProbe>(argument.conf);
}
}
