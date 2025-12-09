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

#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramProbeLogicalOperator.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

EquiWidthHistogramProbeLogicalOperator::EquiWidthHistogramProbeLogicalOperator(
    const uint64_t statisticHash, DataType counterType, DataType startEndType)
    : statisticHash(statisticHash), counterType(counterType), startEndType(startEndType)
{
}

EquiWidthHistogramProbeLogicalOperator::EquiWidthHistogramProbeLogicalOperator(
    uint64_t stashHash,
    DataType counterType,
    DataType startEndType,
    std::string binStartFieldName,
    std::string binEndFieldName,
    std::string binCounterFieldName,
    LogicalStatisticFields logicalStatisticFields)
    : LogicalStatisticFields(std::move(logicalStatisticFields))
    , statisticHash(stashHash)
    , counterType(counterType)
    , startEndType(startEndType)
    , binStartFieldName(std::move(binStartFieldName))
    , binEndFieldName(std::move(binEndFieldName))
    , binCounterFieldName(std::move(binCounterFieldName))
{
}

std::string_view EquiWidthHistogramProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool EquiWidthHistogramProbeLogicalOperator::operator==(const EquiWidthHistogramProbeLogicalOperator& rhs) const
{
    return statisticHash == rhs.statisticHash and counterType == rhs.counterType and startEndType == rhs.startEndType
        and inputSchema == rhs.inputSchema and outputSchema == rhs.outputSchema and inputOriginIds == rhs.inputOriginIds
        and outputOriginIds == rhs.outputOriginIds;
};

EquiWidthHistogramProbeLogicalOperator
EquiWidthHistogramProbeLogicalOperator::withInferredSchema(const std::vector<Schema>& inputSchemas) const
{
    auto copy = *this;
    INVARIANT(inputSchemas.size() == 1, "EquiWidthProbe should have one input schema but got {}", inputSchemas.size());
    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for EquiWidthProbe operator");
        }
    }

    /// EquiWidthHistogramProbeLogicalOperator expects the following fields in its input schema. If not, we need to throw
    copy.inputSchema = firstSchema;
    if (not copy.inputSchema.getFieldByName(copy.statisticStartTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copy.statisticEndTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copy.statisticHashField.name).has_value())
    {
        std::stringstream expectedFields;
        expectedFields << copy.statisticStartTsField << ", " << copy.statisticEndTsField << ", " << copy.statisticHashField;
        throw FieldNotFound("Expected the following fields {} to be in the schema {}.", expectedFields.str(), copy.inputSchema);
    }

    const auto& newQualifierForSystemField = firstSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();

    auto addIfMissing = [](std::string s, const std::string& sub) { return s.find(sub) != std::string::npos ? s : sub + s; };

    copy.binStartFieldName = addIfMissing(this->binStartFieldName, newQualifierForSystemField);
    copy.binEndFieldName = addIfMissing(this->binEndFieldName, newQualifierForSystemField);
    copy.binCounterFieldName = addIfMissing(this->binCounterFieldName, newQualifierForSystemField);

    copy.outputSchema = Schema{};
    copy.statisticHashField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticStartTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticEndTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticNumberOfSeenTuplesField.addQualifierIfNotExists(newQualifierForSystemField);

    copy.outputSchema.addField(copy.statisticHashField);
    copy.outputSchema.addField(copy.statisticStartTsField);
    copy.outputSchema.addField(copy.statisticEndTsField);
    copy.outputSchema.addField(copy.statisticNumberOfSeenTuplesField);

    auto start = Schema::Field(copy.binStartFieldName, DataType{startEndType});
    start.addQualifierIfNotExists(newQualifierForSystemField);
    copy.outputSchema.addField(start);
    auto counter = Schema::Field(copy.binCounterFieldName, DataType{counterType});
    counter.addQualifierIfNotExists(newQualifierForSystemField);
    copy.outputSchema.addField(counter);
    auto end = Schema::Field(copy.binEndFieldName, DataType{startEndType});
    end.addQualifierIfNotExists(newQualifierForSystemField);
    copy.outputSchema.addField(end);


    return copy;
}

EquiWidthHistogramProbeLogicalOperator EquiWidthHistogramProbeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet EquiWidthHistogramProbeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

EquiWidthHistogramProbeLogicalOperator EquiWidthHistogramProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> EquiWidthHistogramProbeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema EquiWidthHistogramProbeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> EquiWidthHistogramProbeLogicalOperator::getChildren() const
{
    return children;
}

std::string EquiWidthHistogramProbeLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "EQUIWIDTH_PROBE(opId: {}, statHash: {}, counterType: {}, startEndType: {})", id, statisticHash, counterType, startEndType);
    }
    return fmt::format("EQUIWIDTH_PROBE()", statisticHash);
}

Reflected Reflector<EquiWidthHistogramProbeLogicalOperator>::operator()(const EquiWidthHistogramProbeLogicalOperator& op) const
{
    return reflect(detail::ReflectedEquiWidthHistogramProbeLogicalOperator{
        .statisticHash = op.statisticHash,
        .counterTypeValue = static_cast<uint8_t>(op.counterType.type),
        .startEndTypeValue = static_cast<uint8_t>(op.startEndType.type),
        .binStartFieldName = op.binStartFieldName,
        .binEndFieldName = op.binEndFieldName,
        .binCounterFieldName = op.binCounterFieldName});
}

EquiWidthHistogramProbeLogicalOperator Unreflector<EquiWidthHistogramProbeLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto data = unreflect<detail::ReflectedEquiWidthHistogramProbeLogicalOperator>(reflected);
    return EquiWidthHistogramProbeLogicalOperator{
        data.statisticHash,
        DataType{static_cast<DataType::Type>(data.counterTypeValue)},
        DataType{static_cast<DataType::Type>(data.startEndTypeValue)},
        std::move(data.binStartFieldName),
        std::move(data.binEndFieldName),
        std::move(data.binCounterFieldName),
        LogicalStatisticFields{}};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterEquiWidthHistogramProbeLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<EquiWidthHistogramProbeLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Expected arguments are missing");
    std::unreachable();
}

}
