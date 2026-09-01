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

#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <CollectionDomain.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/OctetLengthLogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Statistic/ScalarStatisticProbeLogicalOperator.hpp>
#include <Identifiers/Identifier.hpp>
#include <Metric.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/ScalarStatisticAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <RequestStatisticStatement.hpp>
#include <Statistic.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// Only Average is implemented. The remaining metrics map to synopsis statistics -- equi-width histogram for
/// MinVal/MaxVal/Selectivity, count-min sketch for Cardinality, and so on -- none of which are part of this port.
StatisticType toStatisticType(const Metric metric)
{
    if (metric == Metric::Average)
    {
        return StatisticType::Avg;
    }
    throw NotImplemented(
        "Metric {} needs a synopsis statistic, which this port does not provide; only Average is supported",
        magic_enum::enum_name(metric));
}

/// The type a probe reconstructs the persisted scalar as. Only Avg is reachable today, but keeping the mapping
/// explicit means adding Count/Sum is a one-line change rather than a hunt.
DataType probeValueTypeFor(const StatisticType op)
{
    switch (op)
    {
        case StatisticType::Avg:
            return DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE);
        case StatisticType::Count:
            return DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
        case StatisticType::Sum:
            break;
    }
    throw NotImplemented("No probe value type is defined for statistic type {}", magic_enum::enum_name(op));
}

Windowing::TimeBasedWindowType windowTypeFor(const RequestStatisticBuildStatement& request)
{
    if (request.windowAdvanceMs.has_value())
    {
        return Windowing::TimeBasedWindowType{
            Windowing::SlidingWindow{Windowing::TimeMeasure{request.windowSizeMs}, Windowing::TimeMeasure{*request.windowAdvanceMs}}};
    }
    return Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{request.windowSizeMs}}};
}

Windowing::TimeCharacteristic timeCharacteristicFor(const RequestStatisticBuildStatement& request)
{
    if (request.eventTimeFieldName.has_value())
    {
        return Windowing::TimeCharacteristic{
            Windowing::UnboundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createEventTime(
                UnboundFieldAccessLogicalFunction{Identifier::parse(*request.eventTimeFieldName)},
                Windowing::TimeUnit::Milliseconds())}};
    }
    return Windowing::TimeCharacteristic{
        Windowing::UnboundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createIngestionTime()}};
}

/// Splits "host:port" as the coordinator reports it. The sink wants the two halves separately.
std::pair<std::string, std::string> splitAddress(const std::string& address)
{
    const auto colon = address.rfind(':');
    if (colon == std::string::npos)
    {
        throw InvalidConfigParameter("Coordinator address '{}' is not in host:port form", address);
    }
    return {address.substr(0, colon), address.substr(colon + 1)};
}

LogicalPlan generateForDataDomain(
    const DataDomain& domain,
    const RequestStatisticBuildStatement& request,
    const Statistic::StatisticId statisticId,
    const std::string& coordinatorAddress)
{
    auto plan = LogicalPlanBuilder::createLogicalPlan(Identifier::parse(domain.logicalSourceName));

    const ScalarStatisticAggregationLogicalFunction statisticFunction{
        TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{UnboundFieldAccessLogicalFunction{Identifier::parse(domain.fieldName)}},
        statisticId,
        toStatisticType(request.metric)};

    /// The result field is named from the id alone, which is how the fused StatisticStoreWriter finds the payload.
    plan = LogicalPlanBuilder::addWindowAggregation(
        plan,
        windowTypeFor(request),
        {WindowedAggregationLogicalOperator::ProjectedAggregation{statisticFunction, statisticDataFieldName(statisticId)}},
        {},
        timeCharacteristicFor(request));

    /// Project to scalar columns before the sink. Three things are going on here.
    ///
    /// The VARSIZED payload must not reach the sink. The sink transports formatted CSV, and the payload is a raw
    /// IEEE-754 double whose bytes can include commas and newlines, which corrupt the row framing. (This shows up
    /// only for some values: 20.0 and 200.0 happen to be byte-safe, arbitrary averages are not.)
    ///
    /// The statisticId has to travel with the report so the coordinator can route it. The fused writer adds it to
    /// the Nautilus record but not to any schema, so it would not survive a pipeline boundary; projecting it as a
    /// constant puts it in the schema, where it does.
    ///
    /// And the payload has to stay *referenced*, which is what OCTET_LENGTH is doing here. The
    /// StatisticStoreWriter is fused into the aggregation's lowering, so it is invisible to the optimizer: an
    /// aggregation whose only output column nothing reads looks dead, and ProjectionPushdown duly emptied the
    /// aggregation list and took the writer's side effect with it. Reading the payload's length keeps the column
    /// live while staying scalar. It is also a useful assertion in its own right -- a scalar statistic is always
    /// 8 bytes.
    ///
    /// All of this runs after the writer, which sits below in the aggregation, so the store still sees the full
    /// record.
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
    projections.emplace_back(
        Identifier::parse(std::string{StatisticFieldNames::STATISTIC_ID}),
        LogicalFunction{ConstantValueLogicalFunction{uint64Type, std::to_string(statisticId.getRawValue())}});
    projections.emplace_back(
        Identifier::parse(std::string{StatisticFieldNames::START_TS}),
        LogicalFunction{UnboundFieldAccessLogicalFunction{Identifier::parse("start")}});
    projections.emplace_back(
        Identifier::parse(std::string{StatisticFieldNames::END_TS}),
        LogicalFunction{UnboundFieldAccessLogicalFunction{Identifier::parse("end")}});
    projections.emplace_back(
        Identifier::parse(std::string{StatisticFieldNames::PAYLOAD_BYTES}),
        LogicalFunction{OctetLengthLogicalFunction{
            LogicalFunction{UnboundFieldAccessLogicalFunction{statisticDataFieldName(statisticId)}}}});
    plan = LogicalPlanBuilder::addProjection(std::move(projections), /*asterisk=*/false, plan);

    /// A trigger carrying a *predicate* is the one case where the build query also has to read the store back.
    ///
    /// The predicate is evaluated over the statistic's value, and the value only exists in the store -- the build
    /// chain itself carries it as an opaque VARSIZED payload. So a conditional trigger compiles a probe and a
    /// selection into the same query, and only the windows that satisfy the predicate are reported.
    ///
    /// Without a predicate the query stays write-only: every closed window is reported, carrying just the window
    /// metadata, and a caller that wants values calls getStatistics. A trigger with a callback but no predicate
    /// does not need the probe either, because the callback is handed the window and not the value.
    ///
    /// The probe reads the window bounds from the projected columns above, and supplies STATISTICID and
    /// STATISTICVALUE itself, so its output is exactly what the sink reports.
    const bool probeInBuild = request.conditionTrigger.has_value() and request.conditionTrigger->condition.has_value();
    if (probeInBuild)
    {
        const auto probe = ScalarStatisticProbeLogicalOperator::create(
            statisticId,
            toStatisticType(request.metric),
            probeValueTypeFor(toStatisticType(request.metric)),
            Identifier::parse(std::string{StatisticFieldNames::START_TS}),
            Identifier::parse(std::string{StatisticFieldNames::END_TS}));
        plan = plan.withRootOperators({LogicalOperator{probe}.withChildrenUnsafe(plan.getRootOperators())});
        plan = LogicalPlanBuilder::addSelection(request.conditionTrigger->condition.value(), plan);
    }

    const auto [host, port] = splitAddress(coordinatorAddress);
    return LogicalPlanBuilder::addAnonymousSink(
        Identifier::parse("Grpc"),
        std::nullopt,
        {{Identifier::parse("grpc_host"), host},
         {Identifier::parse("grpc_port"), port},
         {Identifier::parse("OUTPUT_FORMAT"), "CSV"},
         {Identifier::parse("host"), host}},
        {},
        plan);
}

}

LogicalPlan DefaultStatisticQueryGenerator::generateQuery(
    const RequestStatisticBuildStatement& request,
    const Statistic::StatisticId statisticId,
    const std::string& coordinatorAddress) const
{
    return std::visit(
        [&]<typename DomainAlternative>(const DomainAlternative& domain) -> LogicalPlan
        {
            using DomainType = std::decay_t<DomainAlternative>;
            if constexpr (std::is_same_v<DomainType, DataDomain>)
            {
                return generateForDataDomain(domain, request, statisticId, coordinatorAddress);
            }
            else if constexpr (std::is_same_v<DomainType, WorkloadDomain>)
            {
                throw NotImplemented(
                    "Collecting a statistic over the output of query {} operator {} is not implemented; the splice "
                    "machinery it needs is not part of this port.",
                    domain.queryId,
                    domain.operatorId);
            }
            else
            {
                throw NotImplemented(
                    "Collecting infrastructure statistics for worker {} is not implemented; it would need "
                    "infrastructure metric sources that do not exist.",
                    domain.hostId);
            }
        },
        request.domain);
}

}
