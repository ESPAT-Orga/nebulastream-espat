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

#include <DefaultStatisticQueryGenerator.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/SketchDimensions.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <CollectionDomain.hpp>
#include <ErrorHandling.hpp>
#include <Metric.hpp>
#include <RequestStatisticStatement.hpp>
#include <Statistic.hpp>

namespace NES
{

namespace
{

uint64_t getOption(const std::unordered_map<std::string, std::string>& options, const std::string& key, const uint64_t defaultValue)
{
    if (const auto it = options.find(key); it != options.end())
    {
        return std::stoull(it->second);
    }
    return defaultValue;
}

Statistic::StatisticType toStatisticType(const Metric& metric)
{
    /// For now, we perform a simple mapping of metric to statistic type.
    switch (metric)
    {
        case Metric::Cardinality:
        case Metric::Rate:
            return Statistic::StatisticType::Count_Min_Sketch;
        case Metric::MinVal:
        case Metric::MaxVal:
            return Statistic::StatisticType::Equi_Width_Histogram;
        case Metric::Average:
            return Statistic::StatisticType::Reservoir_Sample;
    }
    std::unreachable();
}

std::shared_ptr<WindowAggregationLogicalFunction> createAggregationFunction(
    const FieldAccessLogicalFunction& onField,
    const Metric metric,
    const Statistic::StatisticId statisticId,
    const std::unordered_map<std::string, std::string>& options)
{
    switch (toStatisticType(metric))
    {
        case Statistic::StatisticType::Equi_Width_Histogram: {
            const auto numBuckets = getOption(options, "histogram.buckets", 100);
            const auto minValue = getOption(options, "histogram.min", 0);
            const auto maxValue = getOption(options, "histogram.max", 1000);
            return std::make_shared<WindowAggregationLogicalFunction>(EquiWidthHistogramLogicalFunction{
                onField,
                numBuckets,
                minValue,
                maxValue,
                statisticId,
                DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
        }
        case Statistic::StatisticType::Reservoir_Sample: {
            const auto reservoirSize = getOption(options, "sample.reservoir_size", 1000);
            return std::make_shared<WindowAggregationLogicalFunction>(
                ReservoirSampleLogicalFunction{onField, std::vector{onField}, reservoirSize, statisticId});
        }
        case Statistic::StatisticType::Count_Min_Sketch: {
            const auto columns = getOption(options, "sketch.columns", 256);
            const auto rows = getOption(options, "sketch.rows", 4);
            constexpr uint64_t seed = 42;
            return std::make_shared<WindowAggregationLogicalFunction>(CountMinSketchLogicalFunction{
                onField,
                NumberOfCols{columns},
                NumberOfRows{rows},
                seed,
                DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE),
                statisticId});
        }
    }
    std::unreachable();
}

LogicalPlan generateForDataDomain(
    const DataDomain& domain,
    const RequestStatisticBuildStatement& request,
    const Statistic::StatisticId statisticId,
    const std::string& coordinatorAddress)
{
    PRECONDITION(not coordinatorAddress.empty(), "Required to have a coordinator gRPC address!");

    auto timeChar = request.eventTimeFieldName.has_value()
        ? Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction{*request.eventTimeFieldName})
        : Windowing::TimeCharacteristic::createIngestionTime();
    std::shared_ptr<Windowing::WindowType> windowType;
    if (request.windowAdvanceMs.has_value())
    {
        windowType = std::make_shared<Windowing::SlidingWindow>(
            timeChar, Windowing::TimeMeasure{request.windowSizeMs}, Windowing::TimeMeasure{*request.windowAdvanceMs});
    }
    else
    {
        windowType = std::make_shared<Windowing::TumblingWindow>(timeChar, Windowing::TimeMeasure{request.windowSizeMs});
    }

    const FieldAccessLogicalFunction onField{domain.fieldName};
    auto agg = createAggregationFunction(onField, request.metric, statisticId, request.options);

    /// The build and statistic store writer need to have a connection for the statistic fields, e.g., statisticDataField.
    /// As the field names change during type inference
    const auto logicalStatisticFields = std::make_shared<LogicalStatisticFields>();
    auto plan = LogicalPlanBuilder::createLogicalPlan(domain.logicalSourceName);
    plan = LogicalPlanBuilder::addStatisticBuild(std::move(plan), windowType, {agg}, {}, logicalStatisticFields);
    plan = LogicalPlanBuilder::addStatisticStoreWriter(plan, logicalStatisticFields, statisticId, toStatisticType(request.metric));
    if (request.conditionTrigger.has_value())
    {
        plan = LogicalPlanBuilder::addSelection(request.conditionTrigger->condition, plan);
    }

    /// Append a gRPC sink to send results back to the StatisticCoordinator

    const auto colonPos = coordinatorAddress.find(':');
    const auto sinkHost = coordinatorAddress.substr(0, colonPos);
    const auto sinkPort = coordinatorAddress.substr(colonPos + 1);

    /// The StatisticStoreWriter outputs: startTs, endTs, and statisticId
    Schema grpcSinkSchema;
    const LogicalStatisticFields statisticFields;
    grpcSinkSchema.addField(statisticFields.statisticIdField);
    grpcSinkSchema.addField(statisticFields.statisticStartTsField);
    grpcSinkSchema.addField(statisticFields.statisticEndTsField);
    plan = LogicalPlanBuilder::addInlineSink("Grpc", grpcSinkSchema, {{"grpc_host", sinkHost}, {"grpc_port", sinkPort}}, {}, plan);

    return plan;
}

}

LogicalPlan DefaultStatisticQueryGenerator::generateQuery(
    const RequestStatisticBuildStatement& request, const Statistic::StatisticId statisticId, const std::string& coordinatorAddress) const
{
    return std::visit(
        [&]<typename CollectionDomain>(const CollectionDomain& domain) -> LogicalPlan
        {
            using DomainType = std::decay_t<CollectionDomain>;
            if constexpr (std::is_same_v<DomainType, DataDomain>)
            {
                return generateForDataDomain(domain, request, statisticId, coordinatorAddress);
            }
            else if constexpr (std::is_same_v<DomainType, WorkloadDomain>)
            {
                throw NotImplemented(
                    "REQUEST STATISTIC WORKLOAD is not yet implemented. "
                    "Requires extracting subplans from running queries (query {}, operator {}).",
                    domain.queryId,
                    domain.operatorId);
            }
            else if constexpr (std::is_same_v<DomainType, InfrastructureDomain>)
            {
                throw NotImplemented(
                    "REQUEST STATISTIC INFRASTRUCTURE is not yet implemented. "
                    "Requires infrastructure metric sources for worker {}.",
                    domain.hostId);
            }
        },
        request.domain);
}

}
