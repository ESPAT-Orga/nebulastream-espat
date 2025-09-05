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

#include <Operators/Windows/Aggregations/Synopsis/Histogram/HistogramProbeLogicalOperator.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

HistogramProbeLogicalOperator::HistogramProbeLogicalOperator(FieldAccessLogicalFunction asField, uint64_t numBuckets, uint64_t minValue, uint64_t maxValue)
    : asField(asField), numBuckets(numBuckets), minValue(minValue), maxValue(maxValue)
{
}

std::string_view HistogramProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool HistogramProbeLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    const auto* other = dynamic_cast<const HistogramProbeLogicalOperator*>(&rhs);
    return other != nullptr;
};

LogicalOperator HistogramProbeLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    INVARIANT(inputSchemas.size() == 1, "HistogramProbe should have one input schema but got {}", inputSchemas.size());

    auto copy = *this;

    copy.inputSchema = inputSchemas[0];

    auto asFieldName = copy.asField.getFieldName();
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos)
    {
        copy.asField = copy.asField.withFieldName(asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        copy.asField = copy.asField.withFieldName(copy.inputSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator() + asFieldName)
                           .get<FieldAccessLogicalFunction>();
    }

    auto pos = asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR);
    auto asFieldWithoutStream = (pos != std::string::npos) ? asFieldName.substr(pos + 1) : asFieldName;

    copy.outputSchema = Schema{inputSchemas[0].memoryLayoutType};
    for (auto field : copy.inputSchema)
    {
        auto pos = field.name.find(Schema::ATTRIBUTE_NAME_SEPARATOR);
        auto fieldWithoutStream = (pos != std::string::npos) ? field.name.substr(pos + 1) : field.name;

        if (fieldWithoutStream != asFieldWithoutStream)
        {
            copy.outputSchema.addField(field.name, field.dataType);
        }
    }
    uint64_t lower = minValue;
    uint64_t bucketWidth = (maxValue - minValue) / numBuckets;
    for (size_t i = 0; i < numBuckets; i++)
    {
        /// TODO Account for empty buckets - the physical operator EquiWidthHistogram does not put them into its result if empty.
        auto name = fmt::format("bucket[{},{})", lower, lower + bucketWidth);
        copy.outputSchema.addField(name, DataType::Type::UINT64);
        lower += bucketWidth;
    }

    return copy;
}

LogicalOperator HistogramProbeLogicalOperator::withTraitSet(TraitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet HistogramProbeLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator HistogramProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> HistogramProbeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema HistogramProbeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> HistogramProbeLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> HistogramProbeLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator HistogramProbeLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "HistogramProbe should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator HistogramProbeLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> HistogramProbeLogicalOperator::getChildren() const
{
    return children;
}

std::string HistogramProbeLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("HISTOGRAM_PROBE(opId: {}, asField: {}, numBuckets: {}, minValue: {}, maxValue: {})", id, asField, numBuckets, minValue, maxValue);
    }
    return fmt::format("HISTOGRAM_PROBE");
}

void HistogramProbeLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (const auto& trait : getTraitSet())
    {
        *traitSetProto->add_traits() = trait.serialize();
    }

    const auto inputs = getInputSchemas();
    const auto originLists = getInputOriginIds();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);

        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originLists[i])
        {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds())
    {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    serializableOperator.set_operator_id(id.getRawValue());
    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    auto asFieldFn = SerializableFunction();
    asFieldFn.CopyFrom(asField.serialize());
    (*serializableOperator.mutable_config())[ConfigParameters::AS_FIELD] = descriptorConfigTypeToProto(asFieldFn);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterHistogramProbeLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto asFieldSerialized = arguments.config[HistogramProbeLogicalOperator::ConfigParameters::AS_FIELD];
    if (not std::holds_alternative<NES::SerializableFunction>(asFieldSerialized))
    {
        throw UnknownLogicalOperator();
    }
    auto asFieldFunction = FunctionSerializationUtil::deserializeFunction(std::get<NES::SerializableFunction>(asFieldSerialized));
    auto asField = asFieldFunction.get<FieldAccessLogicalFunction>();

    auto logicalOperator = HistogramProbeLogicalOperator(asField, 5, 0, 25);
    if (auto& id = arguments.id)
    {
        logicalOperator.id = *id;
    }
    return logicalOperator.withInferredSchema(arguments.inputSchemas)
        .withInputOriginIds(arguments.inputOriginIds)
        .withOutputOriginIds(arguments.outputOriginIds);
}

}
