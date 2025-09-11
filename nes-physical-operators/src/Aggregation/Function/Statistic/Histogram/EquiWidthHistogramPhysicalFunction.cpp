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

#include <Aggregation/Function/Statistic/Histogram/EquiWidthHistogramPhysicalFunction.hpp>


#include <cstdint>
#include <numeric>
#include <random>
#include <ranges>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>

namespace NES
{
void EquiWidthHistogramPhysicalFunction::lift(const nautilus::val<AggregationState*>&, PipelineMemoryProvider&, const Nautilus::Record&)
{
}

void EquiWidthHistogramPhysicalFunction::combine(
    nautilus::val<AggregationState*>, nautilus::val<AggregationState*>, PipelineMemoryProvider&)
{
}

Nautilus::Record EquiWidthHistogramPhysicalFunction::lower(nautilus::val<AggregationState*>, PipelineMemoryProvider&)
{
    return Nautilus::Record{};
}

void EquiWidthHistogramPhysicalFunction::reset(nautilus::val<AggregationState*>, PipelineMemoryProvider&)
{
}

void EquiWidthHistogramPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t EquiWidthHistogramPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::ChainedHashMap);
}

EquiWidthHistogramPhysicalFunction::EquiWidthHistogramPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    const uint64_t binWidth)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , binWidth(binWidth)
{
}

EquiWidthHistogramPhysicalFunction::~EquiWidthHistogramPhysicalFunction()
{
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterEquiWidthHistogramAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.binWidth.has_value(), "Bin width is not set");
    return std::make_shared<EquiWidthHistogramPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.binWidth.value());
}

}
