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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalScalarStatisticProbe.hpp>

#include <Operators/Windows/Aggregations/Scalar/ScalarStatisticProbeLogicalOperator.hpp>
#include <Statistic/Scalar/ScalarStatisticIteratorImpl.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticStore/StatisticStoreReader.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>

namespace NES
{

namespace
{
StatisticProvider getStatisticProvider(const Statistic::StatisticType op, const DataType valueType, std::string valueFieldName)
{
    auto statisticProviderArguments = std::make_unique<ScalarStatisticProviderArguments>(valueType, std::move(valueFieldName));
    return {op, std::move(statisticProviderArguments)};
}
}

LoweringRuleResultSubgraph
LowerToPhysicalScalarStatisticProbe::apply(LogicalOperator logicalOperator, const std::shared_ptr<AbstractStatisticStore>& statisticStore)
{
    PRECONDITION(logicalOperator.tryGetAs<ScalarStatisticProbeLogicalOperator>(), "Expected a ScalarStatisticProbeLogicalOperator");
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    const auto scalarProbe = logicalOperator.getAs<ScalarStatisticProbeLogicalOperator>();
    auto statisticStoreReaderOperatorHandler = std::make_shared<StatisticStoreOperatorHandler>(statisticStore);
    const auto operatorHandlerId = getNextOperatorHandlerId();
    auto statisticProvider = getStatisticProvider(scalarProbe->op, scalarProbe->valueType, scalarProbe->valueFieldName);
    StatisticStoreReader statisticStoreReader{
        operatorHandlerId,
        scalarProbe->statisticIdField.name,
        scalarProbe->statisticStartTsField.name,
        scalarProbe->statisticEndTsField.name,
        scalarProbe->statisticNumberOfSeenTuplesField.name,
        std::move(statisticProvider)};

    auto inputSchema = scalarProbe.getInputSchemas()[0];
    auto outputSchema = scalarProbe.getOutputSchema();
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
    const std::vector leaves(scalarProbe.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leaves}};
}

LoweringRuleRegistryReturnType
LoweringRuleGeneratedRegistrar::RegisterScalarStatisticProbeLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalScalarStatisticProbe>(argument.conf);
}
}
