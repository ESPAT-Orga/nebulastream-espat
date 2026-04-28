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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalStatisticBuild.hpp>

#include <cstdint>
#include <memory>
#include <numeric>
#include <ranges>
#include <utility>
#include <vector>

#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp>
#include <Operators/Windows/StatisticBuildLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <magic_enum/magic_enum.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <HashMapOptions.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

static std::pair<std::vector<Record::RecordFieldIdentifier>, std::vector<Record::RecordFieldIdentifier>>
getKeyAndValueFields(const StatisticBuildLogicalOperator& logicalOperator)
{
    std::vector<Record::RecordFieldIdentifier> fieldKeyNames;
    std::vector<Record::RecordFieldIdentifier> fieldValueNames;

    /// Getting the key and value field names
    for (const auto& nodeAccess : logicalOperator.getGroupingKeys())
    {
        fieldKeyNames.emplace_back(nodeAccess.getFieldName());
    }
    for (const auto& descriptor : logicalOperator.getWindowAggregation())
    {
        const auto aggregationResultFieldIdentifier = descriptor->getOnField().getFieldName();
        fieldValueNames.emplace_back(aggregationResultFieldIdentifier);
    }
    return {fieldKeyNames, fieldValueNames};
}

static std::unique_ptr<TimeFunction> getTimeFunction(const StatisticBuildLogicalOperator& logicalOperator)
{
    auto* const timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(logicalOperator.getWindowType().get());
    if (timeWindow == nullptr)
    {
        throw UnknownWindowType("Window type is not a time based window type");
    }

    switch (timeWindow->getTimeCharacteristic().getType())
    {
        case Windowing::TimeCharacteristic::Type::IngestionTime: {
            if (timeWindow->getTimeCharacteristic().field.name == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
            {
                return std::make_unique<IngestionTimeFunction>();
            }
            throw UnknownWindowType(
                "The ingestion time field of a window must be: {}", Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME);
        }
        case Windowing::TimeCharacteristic::Type::EventTime: {
            auto timeCharacteristicField = timeWindow->getTimeCharacteristic().field.name;
            auto timeStampField = FieldAccessPhysicalFunction(timeCharacteristicField);
            return std::make_unique<EventTimeFunction>(timeStampField, timeWindow->getTimeCharacteristic().getTimeUnit());
        }
        default: {
            throw UnknownWindowType("Unknown window type: {}", magic_enum::enum_name(timeWindow->getTimeCharacteristic().getType()));
        }
    }
}

namespace
{
std::vector<std::shared_ptr<AggregationPhysicalFunction>> getAggregationPhysicalFunctions(
    const StatisticBuildLogicalOperator& logicalOperator,
    const QueryExecutionConfiguration& configuration,
    MemoryLayoutType memoryLayoutType)
{
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();
    for (const auto& descriptor : aggregationDescriptors)
    {
        auto physicalInputType = DataTypeProvider::provideDataType(descriptor->getInputStamp().type);
        auto physicalFinalType = DataTypeProvider::provideDataType(descriptor->getFinalAggregateStamp().type);

        auto aggregationInputFunction = QueryCompilation::FunctionProvider::lowerFunction(descriptor->getOnField());
        const auto resultFieldIdentifier = descriptor->getAsField().getFieldName();
        auto bufferRef
            = LowerSchemaProvider::lowerSchema(configuration.pageSize.getValue(), logicalOperator.getInputSchemas()[0], memoryLayoutType);

        auto name = descriptor->getName();
        AggregationPhysicalFunctionRegistryArguments aggregationArguments{
            std::move(physicalInputType),
            std::move(physicalFinalType),
            std::move(aggregationInputFunction),
            resultFieldIdentifier,
            bufferRef};

        /// We should think about another way to store the additional arguments for each statistic physical function.
        /// The current approach requries us to add here an if block for every new statistic build physical function.
        /// One idea could be to use a similar approach as with the logical operators, storing all configs in a hashmap.
        if (name.contains("ReservoirSample"))
        {
            const auto logicalReservoirSample = descriptor->tryGetAs<ReservoirSampleLogicalFunction>();
            if (logicalReservoirSample.has_value())
            {
                aggregationArguments.numberOfSeenTuplesFieldName = logicalOperator.getNumberOfSeenTuplesFieldName();
                aggregationArguments.sampleSize = logicalReservoirSample->get().reservoirSize;
                aggregationArguments.seed = logicalReservoirSample->get().seed;
            }
        }
        else if (name.contains("EquiWidthHistogram"))
        {
            const auto logicalEquiWidthHistogram = descriptor->tryGetAs<EquiWidthHistogramLogicalFunction>();
            if (logicalEquiWidthHistogram.has_value())
            {
                aggregationArguments.numberOfSeenTuplesFieldName = logicalOperator.getNumberOfSeenTuplesFieldName();
                aggregationArguments.counterType = logicalEquiWidthHistogram->get().counterType;
                aggregationArguments.minValue = logicalEquiWidthHistogram->get().minValue;
                aggregationArguments.maxValue = logicalEquiWidthHistogram->get().maxValue;
                aggregationArguments.numberOfBins = logicalEquiWidthHistogram->get().numBuckets;
            }
        }
        else if (name.contains("CountMinSketch"))
        {
            const auto logicalCountMinSketch = descriptor->tryGetAs<CountMinSketchLogicalFunction>();
            if (logicalCountMinSketch.has_value())
            {
                aggregationArguments.numberOfSeenTuplesFieldName = logicalOperator.getNumberOfSeenTuplesFieldName();
                aggregationArguments.counterType = logicalCountMinSketch->get().counterType;
                aggregationArguments.columns = logicalCountMinSketch->get().columns;
                aggregationArguments.rows = logicalCountMinSketch->get().rows;
                aggregationArguments.seed = logicalCountMinSketch->get().seed;
            }
        }

        if (auto aggregationPhysicalFunction
            = AggregationPhysicalFunctionRegistry::instance().create(std::string(name), std::move(aggregationArguments)))
        {
            aggregationPhysicalFunctions.push_back(aggregationPhysicalFunction.value());
        }
        else
        {
            throw UnknownAggregationType("unknown statistic type: {}", name);
        }
    }
    return aggregationPhysicalFunctions;
}
}

LoweringRuleResultSubgraph LowerToPhysicalStatisticBuild::apply(
    LogicalOperator logicalOperator, [[maybe_unused]] const std::shared_ptr<AbstractStatisticStore>& statisticStore)
{
    PRECONDITION(logicalOperator.tryGetAs<StatisticBuildLogicalOperator>(), "Expected a StatisticBuildLogicalOperator");
    PRECONDITION(std::ranges::size(logicalOperator.getChildren()) == 1, "Expected one child");
    auto outputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(logicalOperator.getTraitSet());
    auto inputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(logicalOperator.getChildren().at(0).getTraitSet());
    PRECONDITION(outputOriginIdsOpt.has_value(), "Expected the outputOriginIds trait to be set");
    PRECONDITION(inputOriginIdsOpt.has_value(), "Expected the inputOriginIds trait to be set");
    const auto& outputOriginIds = outputOriginIdsOpt.value().get();
    PRECONDITION(std::ranges::size(outputOriginIds) == 1, "Expected one output origin id");
    PRECONDITION(logicalOperator.getInputSchemas().size() == 1, "Expected one input schema");
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    auto aggregation = logicalOperator.getAs<StatisticBuildLogicalOperator>();
    auto handlerId = getNextOperatorHandlerId();
    auto outputSchema = aggregation.getOutputSchema();
    auto outputOriginId = outputOriginIds[0];
    auto inputOriginIds = inputOriginIdsOpt.value().get();
    auto windowType = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(aggregation->getWindowType());
    auto timeFunction = getTimeFunction(*aggregation);

    auto aggregationPhysicalFunctions = getAggregationPhysicalFunctions(*aggregation, conf, memoryLayoutType);

    const auto valueSize = std::accumulate(
        aggregationPhysicalFunctions.begin(),
        aggregationPhysicalFunctions.end(),
        0,
        [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

    uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    auto newInputSchema = aggregation.getInputSchemas()[0];
    for (auto& nodeFunctionKey : aggregation->getGroupingKeys())
    {
        auto loweredFunctionType = nodeFunctionKey.getDataType();
        if (loweredFunctionType.isType(DataType::Type::VARSIZED))
        {
            const bool fieldReplaceSuccess = newInputSchema.replaceTypeOfField(nodeFunctionKey.getFieldName(), loweredFunctionType);
            INVARIANT(fieldReplaceSuccess, "Expect to change the type of {} for {}", nodeFunctionKey.getFieldName(), newInputSchema);
        }
        keyFunctions.emplace_back(QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionKey));
        keySize += DataTypeProvider::provideDataType(loweredFunctionType.type).getSizeInBytesWithoutNull();
    }
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    const auto numberOfBuckets = conf.numberOfPartitions.getValue();
    const auto pageSize = std::max(conf.pageSize.getValue(), entrySize * conf.minEntriesPerPage.getValue());
    const auto entriesPerPage = pageSize / entrySize;

    const auto& [fieldKeyNames, fieldValueNames] = getKeyAndValueFields(*aggregation);
    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(newInputSchema, fieldKeyNames, fieldValueNames);

    const auto windowMetaData = WindowMetaData{aggregation->getWindowStartFieldName(), aggregation->getWindowEndFieldName()};

    const HashMapOptions hashMapOptions(
        std::make_unique<MurMur3HashFunction>(),
        keyFunctions,
        fieldKeys,
        fieldValues,
        entriesPerPage,
        entrySize,
        keySize,
        valueSize,
        pageSize,
        numberOfBuckets);

    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType->getSize().getTime(), windowType->getSlide().getTime(), conf.sliceCacheConfiguration);
    auto sliceStoreRef = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId) -> std::span<const std::byte>
        {
            auto& aggregationSlice = dynamic_cast<AggregationSlice&>(slice);
            auto* ptr = aggregationSlice.getHashMapPtrOrCreate(workerThreadId);
            return {reinterpret_cast<const std::byte*>(ptr), sizeof(ChainedHashMap)};
        },
        [hashMapOptions](WindowBasedOperatorHandler& handler)
        {
            auto& aggHandler = dynamic_cast<AggregationOperatorHandler&>(handler);
            const CreateNewHashMapSliceArgs hashMapSliceArgs{
                {aggHandler.cleanupStateNautilusFunction},
                hashMapOptions.keySize,
                hashMapOptions.valueSize,
                hashMapOptions.pageSize,
                hashMapOptions.numberOfBuckets};
            return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
        });
    auto handler = std::make_shared<AggregationOperatorHandler>(
        inputOriginIds | std::ranges::to<std::vector>(), outputOriginId, std::move(sliceAndWindowStore), conf.maxNumberOfBuckets);
    const AggregationBuildPhysicalOperator build{
        handlerId, std::move(timeFunction), std::move(sliceStoreRef), aggregationPhysicalFunctions, hashMapOptions};
    const AggregationProbePhysicalOperator probe{hashMapOptions, aggregationPhysicalFunctions, handlerId, windowMetaData};

    auto buildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        build,
        newInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        probe,
        newInputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{buildWrapper});

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leaves(logicalOperator.getChildren().size(), buildWrapper);
    return {.root = probeWrapper, .leafs = {leaves}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterStatisticBuildLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalStatisticBuild>(argument.conf);
}
}
