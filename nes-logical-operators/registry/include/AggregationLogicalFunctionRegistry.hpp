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

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using AggregationLogicalFunctionRegistryReturnType = std::shared_ptr<WindowAggregationLogicalFunction>;

struct AggregationLogicalFunctionRegistryArguments
{
    std::vector<FieldAccessLogicalFunction> fields;
    std::optional<uint64_t> reservoirSize;
    std::optional<uint64_t> histogramNumBuckets;
    std::optional<uint64_t> histogramMinValue;
    std::optional<uint64_t> histogramMaxValue;
    std::optional<uint64_t> countMinNumColumns;
    std::optional<uint64_t> countMinNumRows;
};

class AggregationLogicalFunctionRegistry : public BaseRegistry<
                                               AggregationLogicalFunctionRegistry,
                                               std::string,
                                               AggregationLogicalFunctionRegistryReturnType,
                                               AggregationLogicalFunctionRegistryArguments>
{
};
}

#define INCLUDED_FROM_REGISTRY_WINDOW_AGGREGATION_FUNCTION
#include <AggregationLogicalFunctionGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_WINDOW_AGGREGATION_FUNCTION
