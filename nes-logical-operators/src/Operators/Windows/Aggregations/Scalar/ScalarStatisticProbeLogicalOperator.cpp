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

#include <Operators/Windows/Aggregations/Scalar/ScalarStatisticProbeLogicalOperator.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Statistic.hpp>

namespace NES
{

ScalarStatisticProbeLogicalOperator::ScalarStatisticProbeLogicalOperator(
    const Statistic::StatisticId statisticId, const Statistic::StatisticType op, DataType valueType)
    : statisticId(statisticId), op(op), valueType(std::move(valueType))
{
}

ScalarStatisticProbeLogicalOperator::ScalarStatisticProbeLogicalOperator(
    const Statistic::StatisticId statisticId,
    const Statistic::StatisticType op,
    DataType valueType,
    std::string valueFieldName,
    LogicalStatisticFields logicalStatisticFields)
    : LogicalStatisticFields(std::move(logicalStatisticFields))
    , statisticId(statisticId)
    , op(op)
    , valueType(std::move(valueType))
    , valueFieldName(std::move(valueFieldName))
{
}

std::string_view ScalarStatisticProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool ScalarStatisticProbeLogicalOperator::operator==(const ScalarStatisticProbeLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId and op == rhs.op and valueType == rhs.valueType and inputSchema == rhs.inputSchema
        and outputSchema == rhs.outputSchema and inputOriginIds == rhs.inputOriginIds and outputOriginIds == rhs.outputOriginIds;
};

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withInferredSchema(const std::vector<Schema>& inputSchemas) const
{
    auto copy = *this;
    INVARIANT(inputSchemas.size() == 1, "ScalarStatisticProbe should have one input schema but got {}", inputSchemas.size());
    const auto& firstSchema = inputSchemas[0];
    const bool allEqual = std::ranges::adjacent_find(inputSchemas, std::ranges::not_equal_to{}) == inputSchemas.end();
    if (not allEqual)
    {
        throw CannotInferSchema("All input schemas must be equal for ScalarStatisticProbe operator");
    }

    /// ScalarStatisticProbeLogicalOperator expects the following fields in its input schema. If not, we need to throw
    copy.inputSchema = firstSchema;
    if (not copy.inputSchema.getFieldByName(copy.statisticStartTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copy.statisticEndTsField.name).has_value()
        or not copy.inputSchema.getFieldByName(copy.statisticIdField.name).has_value())
    {
        std::stringstream expectedFields;
        expectedFields << copy.statisticStartTsField << ", " << copy.statisticEndTsField << ", " << copy.statisticIdField;
        throw FieldNotFound("Expected the following fields {} to be in the schema {}.", expectedFields.str(), copy.inputSchema);
    }

    const auto& newQualifierForSystemField = firstSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();

    auto addIfMissing = [](std::string s, const std::string& sub) { return s.find(sub) != std::string::npos ? s : sub + s; };

    copy.valueFieldName = addIfMissing(this->valueFieldName, newQualifierForSystemField);

    copy.outputSchema = Schema{};
    copy.statisticIdField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticStartTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticEndTsField.addQualifierIfNotExists(newQualifierForSystemField);
    copy.statisticNumberOfSeenTuplesField.addQualifierIfNotExists(newQualifierForSystemField);

    copy.outputSchema.addField(copy.statisticIdField);
    copy.outputSchema.addField(copy.statisticStartTsField);
    copy.outputSchema.addField(copy.statisticEndTsField);
    copy.outputSchema.addField(copy.statisticNumberOfSeenTuplesField);

    Schema::Field value(copy.valueFieldName, copy.valueType);
    value.addQualifierIfNotExists(newQualifierForSystemField);
    copy.outputSchema.addField(value);

    return copy;
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet ScalarStatisticProbeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

ScalarStatisticProbeLogicalOperator ScalarStatisticProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> ScalarStatisticProbeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema ScalarStatisticProbeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> ScalarStatisticProbeLogicalOperator::getChildren() const
{
    return children;
}

std::string ScalarStatisticProbeLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SCALARSTATISTIC_PROBE(opId: {}, statId: {}, op: {}, valueType: {})", id, statisticId, magic_enum::enum_name(op), valueType);
    }
    return fmt::format("SCALARSTATISTIC_PROBE()");
}

Reflected Reflector<ScalarStatisticProbeLogicalOperator>::operator()(const ScalarStatisticProbeLogicalOperator& op) const
{
    return reflect(detail::ReflectedScalarStatisticProbeLogicalOperator{
        .statisticId = op.statisticId.getRawValue(),
        .op = static_cast<uint64_t>(op.op),
        .valueType = op.valueType,
        .valueFieldName = op.valueFieldName});
}

ScalarStatisticProbeLogicalOperator Unreflector<ScalarStatisticProbeLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto data = unreflect<detail::ReflectedScalarStatisticProbeLogicalOperator>(reflected);
    return ScalarStatisticProbeLogicalOperator{
        Statistic::StatisticId{data.statisticId},
        static_cast<Statistic::StatisticType>(data.op),
        data.valueType,
        std::move(data.valueFieldName),
        LogicalStatisticFields{}};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterScalarStatisticProbeLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ScalarStatisticProbeLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Expected arguments are missing");
    std::unreachable();
}

}
