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

#include <Operators/Windows/StatisticBuildLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{


StatisticBuildLogicalOperator::StatisticBuildLogicalOperator(
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
    std::shared_ptr<Windowing::WindowType> windowType,
    std::shared_ptr<LogicalStatisticFields> logicalStatisticFields)
    : aggregationFunctions(std::move(aggregationFunctions))
    , windowType(std::move(windowType))
    , logicalStatisticFields(std::move(logicalStatisticFields))
{
    PRECONDITION(this->logicalStatisticFields != nullptr, "A StatisticBuild operator always needs logicalStatisticFields.");
}

std::string_view StatisticBuildLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string StatisticBuildLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        auto windowType = getWindowType();
        auto windowAggregation = getWindowAggregation();
        return fmt::format(
            "STATISTIC BUILD(opId: {}, {}, window type: {})",
            id,
            fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->toString(); }), ", "),
            windowType->toString());
    }
    auto windowAggregation = getWindowAggregation();
    return fmt::format(
        "STAT BUILD({})", fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->getName(); }), ", "));
}

bool StatisticBuildLogicalOperator::operator==(const StatisticBuildLogicalOperator& rhs) const
{
    const auto rhsWindowAggregation = rhs.getWindowAggregation();
    if (aggregationFunctions.size() != rhsWindowAggregation.size())
    {
        return false;
    }

    for (uint64_t i = 0; i < aggregationFunctions.size(); i++)
    {
        if (*aggregationFunctions[i] != rhsWindowAggregation[i])
        {
            return false;
        }
    }
    if (*logicalStatisticFields != *rhs.logicalStatisticFields)
    {
        return false;
    }

    return *windowType == *rhs.getWindowType() && getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas()
        && getTraitSet() == rhs.getTraitSet();
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "StatisticBuild should have at least one input");

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for StatisticBuild operator");
        }
    }

    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> newFunctions;
    for (const auto& agg : getWindowAggregation())
    {
        agg->inferStamp(firstSchema);
        newFunctions.push_back(agg);
    }
    copy.aggregationFunctions = newFunctions;

    copy.windowType->inferStamp(firstSchema);
    copy.inputSchema = firstSchema;
    copy.outputSchema = Schema{copy.outputSchema.memoryLayoutType};

    const auto& newQualifierForSystemField = firstSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();
    copy.logicalStatisticFields->addQualifierName(newQualifierForSystemField);

    if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(getWindowType().get()))
    {
        copy.windowMetaData.windowStartFieldName = copy.logicalStatisticFields->statisticStartTsField.name;
        copy.windowMetaData.windowEndFieldName = copy.logicalStatisticFields->statisticEndTsField.name;
        copy.outputSchema.addField(copy.windowMetaData.windowStartFieldName, copy.logicalStatisticFields->statisticStartTsField.dataType);
        copy.outputSchema.addField(copy.windowMetaData.windowEndFieldName, copy.logicalStatisticFields->statisticEndTsField.dataType);
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType()->toString());
    }
    for (const auto& agg : copy.aggregationFunctions)
    {
        copy.outputSchema.addField(agg->asField.getFieldName(), agg->asField.getDataType());
    }

    if (aggregationFunctions.size() != 1)
    {
        throw CannotInferSchema("Expect exactly one aggregation for a statistic aggregation but found {}", aggregationFunctions.size());
    }
    copy.logicalStatisticFields->statisticDataField
        = {aggregationFunctions[0]->asField.getFieldName(), aggregationFunctions[0]->asField.getDataType()};
    copy.outputSchema.addField(logicalStatisticFields->statisticNumberOfSeenTuplesField);

    return copy;
}

TraitSet StatisticBuildLogicalOperator::getTraitSet() const
{
    return traitSet;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> StatisticBuildLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema StatisticBuildLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> StatisticBuildLogicalOperator::getChildren() const
{
    return children;
}

std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> StatisticBuildLogicalOperator::getWindowAggregation() const
{
    return aggregationFunctions;
}

void StatisticBuildLogicalOperator::setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> wa)
{
    aggregationFunctions = std::move(wa);
}

std::shared_ptr<Windowing::WindowType> StatisticBuildLogicalOperator::getWindowType() const
{
    return windowType;
}

void StatisticBuildLogicalOperator::setWindowType(std::shared_ptr<Windowing::WindowType> wt)
{
    windowType = std::move(wt);
}

std::vector<FieldAccessLogicalFunction> StatisticBuildLogicalOperator::getGroupingKeys() const
{
    return groupingKey;
}

std::string StatisticBuildLogicalOperator::getWindowStartFieldName() const
{
    return windowMetaData.windowStartFieldName;
}

std::string StatisticBuildLogicalOperator::getWindowEndFieldName() const
{
    return windowMetaData.windowEndFieldName;
}

const WindowMetaData& StatisticBuildLogicalOperator::getWindowMetaData() const
{
    return windowMetaData;
}

std::string StatisticBuildLogicalOperator::getNumberOfSeenTuplesFieldName() const
{
    return logicalStatisticFields->statisticNumberOfSeenTuplesField.name;
}

void StatisticBuildLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    const auto inputs = getInputSchemas();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    /// Serialize window aggregations
    AggregationFunctionList aggList;
    for (const auto& agg : getWindowAggregation())
    {
        *aggList.add_functions() = agg->serialize();
    }
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_AGGREGATIONS] = descriptorConfigTypeToProto(aggList);

    /// Serialize window info
    WindowInfos windowInfo;
    if (auto timeBasedWindow = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType))
    {
        auto timeChar = timeBasedWindow->getTimeCharacteristic();
        auto timeCharProto = WindowInfos_TimeCharacteristic();
        timeCharProto.set_type(WindowInfos_TimeCharacteristic_Type_Event_time);
        timeCharProto.set_field(timeChar.field.name);
        timeCharProto.set_multiplier(timeChar.getTimeUnit().getMillisecondsConversionMultiplier());
        windowInfo.mutable_time_characteristic()->CopyFrom(timeCharProto);
        if (auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType))
        {
            auto* tumbling = windowInfo.mutable_tumbling_window();
            tumbling->set_size(tumblingWindow->getSize().getTime());
        }
        else if (auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType))
        {
            auto* sliding = windowInfo.mutable_sliding_window();
            sliding->set_size(slidingWindow->getSize().getTime());
            sliding->set_slide(slidingWindow->getSlide().getTime());
        }
    }
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_INFOS] = descriptorConfigTypeToProto(windowInfo);

    /// Serialize logicalStatisticFields. Also use a function from LogicalStatisticFields here!
    SerializableSchema_SerializableField serField;
    SchemaSerializationUtil::serializeField(logicalStatisticFields->statisticStartTsField, &serField);
    (*serializableOperator.mutable_config())[ConfigParameters::STATISTIC_START_FIELD_NAME] = descriptorConfigTypeToProto(serField);
    SchemaSerializationUtil::serializeField(logicalStatisticFields->statisticEndTsField, &serField);
    (*serializableOperator.mutable_config())[ConfigParameters::STATISTIC_END_FIELD_NAME] = descriptorConfigTypeToProto(serField);
    SchemaSerializationUtil::serializeField(logicalStatisticFields->statisticDataField, &serField);
    (*serializableOperator.mutable_config())[ConfigParameters::STATISTIC_DATA_FIELD_NAME] = descriptorConfigTypeToProto(serField);
    SchemaSerializationUtil::serializeField(logicalStatisticFields->statisticTypeField, &serField);
    (*serializableOperator.mutable_config())[ConfigParameters::STATISTIC_TYPE_FIELD_NAME] = descriptorConfigTypeToProto(serField);
    SchemaSerializationUtil::serializeField(logicalStatisticFields->statisticHashField, &serField);
    (*serializableOperator.mutable_config())[ConfigParameters::STATISTIC_HASH_FIELD_NAME] = descriptorConfigTypeToProto(serField);
    SchemaSerializationUtil::serializeField(logicalStatisticFields->statisticNumberOfSeenTuplesField, &serField);
    (*serializableOperator.mutable_config())[ConfigParameters::STATISTIC_NUMBER_OF_SEEN_TUPLES_FIELD_NAME]
        = descriptorConfigTypeToProto(serField);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterStatisticBuildLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    auto aggregationsVariant = arguments.config[StatisticBuildLogicalOperator::ConfigParameters::WINDOW_AGGREGATIONS];
    auto keysVariant = arguments.config[StatisticBuildLogicalOperator::ConfigParameters::WINDOW_KEYS];
    auto windowInfoVariant = arguments.config[StatisticBuildLogicalOperator::ConfigParameters::WINDOW_INFOS];

    if (!std::holds_alternative<AggregationFunctionList>(aggregationsVariant))
    {
        throw UnknownLogicalOperator();
    }
    auto aggregations = std::get<AggregationFunctionList>(aggregationsVariant).functions();
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregations;
    for (const auto& agg : aggregations)
    {
        auto function = FunctionSerializationUtil::deserializeWindowAggregationFunction(agg);
        windowAggregations.push_back(function);
    }

    std::shared_ptr<Windowing::WindowType> windowType;
    if (std::holds_alternative<WindowInfos>(windowInfoVariant))
    {
        auto windowInfoProto = std::get<WindowInfos>(windowInfoVariant);
        if (windowInfoProto.has_tumbling_window())
        {
            if (windowInfoProto.time_characteristic().type() == WindowInfos_TimeCharacteristic_Type_Ingestion_time)
            {
                auto timeChar = Windowing::TimeCharacteristic::createIngestionTime();
                windowType = std::make_shared<Windowing::TumblingWindow>(
                    timeChar, Windowing::TimeMeasure(windowInfoProto.tumbling_window().size()));
            }
            else
            {
                auto field = FieldAccessLogicalFunction(windowInfoProto.time_characteristic().field());
                auto multiplier = windowInfoProto.time_characteristic().multiplier();
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier));
                windowType = std::make_shared<Windowing::TumblingWindow>(
                    timeChar, Windowing::TimeMeasure(windowInfoProto.tumbling_window().size()));
            }
        }
        else if (windowInfoProto.has_sliding_window())
        {
            if (windowInfoProto.time_characteristic().type() == WindowInfos_TimeCharacteristic_Type_Ingestion_time)
            {
                auto timeChar = Windowing::TimeCharacteristic::createIngestionTime();
                windowType = Windowing::SlidingWindow::of(
                    timeChar,
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().size()),
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().slide()));
            }
            else
            {
                auto field = FieldAccessLogicalFunction(windowInfoProto.time_characteristic().field());
                auto multiplier = windowInfoProto.time_characteristic().multiplier();
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier));
                windowType = Windowing::SlidingWindow::of(
                    timeChar,
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().size()),
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().slide()));
            }
        }
    }
    if (!windowType)
    {
        throw UnknownLogicalOperator();
    }


    /// Do this the same way as it is done in StatisticStoreWriterLogicalOperator, or, even better, write a function (maybe in
    /// LogicalStatisticFields).
    auto windowStartVariant = arguments.config[StatisticBuildLogicalOperator::ConfigParameters::STATISTIC_START_FIELD_NAME];
    auto windowStart = SchemaSerializationUtil::deserializeField(std::get<SerializableSchema_SerializableField>(windowStartVariant));
    auto windowEndVariant = arguments.config[StatisticBuildLogicalOperator::ConfigParameters::STATISTIC_END_FIELD_NAME];
    auto windowEnd = SchemaSerializationUtil::deserializeField(std::get<SerializableSchema_SerializableField>(windowEndVariant));
    auto statisticDataVariant = arguments.config[StatisticBuildLogicalOperator::ConfigParameters::STATISTIC_DATA_FIELD_NAME];
    auto statisticData = SchemaSerializationUtil::deserializeField(std::get<SerializableSchema_SerializableField>(statisticDataVariant));
    auto statisticTypeVariant = arguments.config[StatisticBuildLogicalOperator::ConfigParameters::STATISTIC_TYPE_FIELD_NAME];
    auto statisticType = SchemaSerializationUtil::deserializeField(std::get<SerializableSchema_SerializableField>(statisticTypeVariant));
    auto statisticHashVariant = arguments.config[StatisticBuildLogicalOperator::ConfigParameters::STATISTIC_HASH_FIELD_NAME];
    auto statisticHash = SchemaSerializationUtil::deserializeField(std::get<SerializableSchema_SerializableField>(statisticHashVariant));
    auto statisticNumberofSeenTuplesVariant
        = arguments.config[StatisticBuildLogicalOperator::ConfigParameters::STATISTIC_NUMBER_OF_SEEN_TUPLES_FIELD_NAME];
    auto statisticNumberofSeenTuples
        = SchemaSerializationUtil::deserializeField(std::get<SerializableSchema_SerializableField>(statisticNumberofSeenTuplesVariant));

    auto logicalStatisticFields
        = std::make_shared<LogicalStatisticFields>(statisticNumberofSeenTuples, statisticHash, windowStart, windowEnd);
    logicalStatisticFields->statisticDataField = statisticData;
    logicalStatisticFields->statisticTypeField = statisticType;

    auto logicalOperator = StatisticBuildLogicalOperator(windowAggregations, windowType, logicalStatisticFields);
    if (arguments.inputSchemas.empty())
    {
        throw CannotDeserialize("Cannot construct StatisticBuild");
    }
    return logicalOperator.withInferredSchema(arguments.inputSchemas);
}

}
