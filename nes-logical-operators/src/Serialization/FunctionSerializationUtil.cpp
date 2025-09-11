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

#include <Serialization/FunctionSerializationUtil.hpp>

#include <memory>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES::FunctionSerializationUtil
{

LogicalFunction deserializeFunction(const SerializableFunction& serializedFunction)
{
    const auto& functionType = serializedFunction.function_type();

    std::vector<LogicalFunction> deserializedChildren;
    for (const auto& child : serializedFunction.children())
    {
        deserializedChildren.emplace_back(deserializeFunction(child));
    }

    auto dataType = DataTypeSerializationUtil::deserializeDataType(serializedFunction.data_type());

    DescriptorConfig::Config functionDescriptorConfig{};
    for (const auto& [key, value] : serializedFunction.config())
    {
        functionDescriptorConfig[key] = protoToDescriptorConfigType(value);
    }

    auto argument = LogicalFunctionRegistryArguments(functionDescriptorConfig, deserializedChildren, dataType);

    if (auto function = LogicalFunctionRegistry::instance().create(functionType, argument))
    {
        return function.value();
    }
    throw CannotDeserialize("Logical Function: {}", serializedFunction.DebugString());
}

std::shared_ptr<WindowAggregationLogicalFunction>
deserializeWindowAggregationFunction(const SerializableAggregationFunction& serializedFunction)
{
    const auto& type = serializedFunction.type();
    auto onField = deserializeFunction(serializedFunction.on_field());
    auto asField = deserializeFunction(serializedFunction.as_field());

    AggregationLogicalFunctionRegistryArguments args;
    if (type == "ReservoirSample")
    {
        args.reservoirSize = serializedFunction.reservoir_size();
        auto fieldsFns = serializedFunction.sample_fields().functions();
        args.fields = std::vector{
            fieldsFns
            | std::views::transform(
                [](const auto& fn)
                {
                    const auto logFn = deserializeFunction(fn);
                    return logFn.template get<FieldAccessLogicalFunction>();
                })
            | std::ranges::to<std::vector>()};
        args.numberOfSeenTuplesFieldName = serializedFunction.number_of_seen_tuples_field_name();
    }
    if (type == "EquiWidthHistogram")
    {
        args.histogramMinValue = serializedFunction.histogram_min_value();
        args.histogramMaxValue = serializedFunction.histogram_max_value();
        args.histogramNumBuckets = serializedFunction.histogram_num_buckets();
        args.numberOfSeenTuplesFieldName = serializedFunction.number_of_seen_tuples_field_name();
    }
    if (type == "CountMinSketch")
    {
        args.countMinNumColumns = serializedFunction.count_min_num_columns();
        args.countMinNumRows = serializedFunction.count_min_num_rows();
        args.numberOfSeenTuplesFieldName = serializedFunction.number_of_seen_tuples_field_name();
    }

    if (auto fieldAccess = onField.tryGet<FieldAccessLogicalFunction>())
    {
        if (auto asFieldAccess = asField.tryGet<FieldAccessLogicalFunction>())
        {
            args.fields.insert(args.fields.begin(), {fieldAccess.value(), asFieldAccess.value()});

            if (auto function = AggregationLogicalFunctionRegistry::instance().create(type, args))
            {
                return function.value();
            }
        }
    }
    throw UnknownLogicalOperator();
}

}
