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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalReservoirProbe.hpp>

#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirProbeLogicalOperator.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Statistic/Sample/ReservoirSampleIteratorImpl.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticStore/StatisticStoreReader.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>

namespace NES
{

namespace
{
StatisticProvider getStatisticProvider(const Schema& sampleSchema)
{
    /// Compute field offsets for sample fields in row layout
    std::vector<FieldOffsets> sampleFields;
    uint64_t currentOffset = 0;
    for (const auto& field : sampleSchema.getFields())
    {
        sampleFields.push_back(FieldOffsets{field.name, field.dataType, currentOffset});
        currentOffset += field.dataType.getSizeInBytesWithoutNull();
    }
    auto statisticProviderArguments = std::make_unique<ReservoirSampleProviderArguments>(std::move(sampleFields));
    return {Statistic::StatisticType::Reservoir_Sample, std::move(statisticProviderArguments)};
}
}

LoweringRuleResultSubgraph LowerToPhysicalReservoirProbe::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<ReservoirProbeLogicalOperator>(), "Expected a ReservoirProbeLogicalOperator");
    const auto reservoirProbe = logicalOperator.getAs<ReservoirProbeLogicalOperator>();
    auto statisticStore = NodeEngine::getStatisticStore();
    auto statisticStoreReaderOperatorHandler = std::make_shared<StatisticStoreOperatorHandler>(std::move(statisticStore));
    const auto operatorHandlerId = getNextOperatorHandlerId();
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    auto statisticProvider = getStatisticProvider(reservoirProbe->sampleSchema);
    StatisticStoreReader statisticStoreReader{
        operatorHandlerId,
        reservoirProbe->statisticIdField.name,
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
        memoryLayoutType,
        memoryLayoutType,
        operatorHandlerId,
        statisticStoreReaderOperatorHandler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    const std::vector leafs(reservoirProbe.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafs}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterReservoirProbeLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalReservoirProbe>(argument.conf);
}
}
