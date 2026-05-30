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

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <format>
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
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/SketchDimensions.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
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
            const auto memoryBudget = getOption(options, "memory_budget", 4096);
            const auto minValue = getOption(options, "min", 0);
            const auto maxValue = getOption(options, "max", 1000);
            return std::make_shared<WindowAggregationLogicalFunction>(
                EquiWidthHistogramLogicalFunction{onField, memoryBudget, minValue, maxValue, statisticId});
        }
        case Statistic::StatisticType::Reservoir_Sample: {
            const auto memoryBudget = getOption(options, "memory_budget", 8192);
            return std::make_shared<WindowAggregationLogicalFunction>(
                ReservoirSampleLogicalFunction{onField, std::vector{onField}, memoryBudget, statisticId});
        }
        case Statistic::StatisticType::Count_Min_Sketch: {
            const auto memoryBudget = getOption(options, "memory_budget", 8192);
            return std::make_shared<WindowAggregationLogicalFunction>(CountMinSketchLogicalFunction{onField, memoryBudget, statisticId});
        }
    }
    std::unreachable();
}

/// Build the windowed-aggregation + store-writer + (optional selection) chain on top of `basePlan`,
/// stopping before any sink is attached. The caller appends either a gRPC sink (DataDomain — reports
/// results back to the coordinator per window close) or a VoidSink (WorkloadDomain — the probe query
/// handles coordinator reports separately at a low rate).
LogicalPlan stackBuildChainOnTop(
    LogicalPlan basePlan,
    const std::string& fieldNameUpper,
    const RequestStatisticBuildStatement& request,
    const Statistic::StatisticId statisticId)
{
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

    const FieldAccessLogicalFunction onField{fieldNameUpper};
    auto agg = createAggregationFunction(onField, request.metric, statisticId, request.options);

    /// The build and statistic store writer need to have a connection for the statistic fields, e.g., statisticDataField.
    /// As the field names change during type inference
    const auto logicalStatisticFields = std::make_shared<LogicalStatisticFields>();
    auto plan = std::move(basePlan);
    plan = LogicalPlanBuilder::addStatisticBuild(std::move(plan), windowType, {agg}, {}, logicalStatisticFields);
    plan = LogicalPlanBuilder::addStatisticStoreWriter(plan, logicalStatisticFields, statisticId, toStatisticType(request.metric));
    if (request.conditionTrigger.has_value() && request.conditionTrigger->condition.has_value())
    {
        plan = LogicalPlanBuilder::addSelection(*request.conditionTrigger->condition, plan);
    }
    return plan;
}

/// Append a gRPC sink to the statistic chain so the coordinator receives results per window close.
LogicalPlan appendGrpcSinkToStatisticChain(
    LogicalPlan plan,
    const std::string& sourceNameUpper,
    const std::string& coordinatorAddress,
    const std::unordered_map<std::string, std::string>& options)
{
    PRECONDITION(not coordinatorAddress.empty(), "Required to have a coordinator gRPC address!");
    const auto colonPos = coordinatorAddress.find(':');
    const auto sinkHost = coordinatorAddress.substr(0, colonPos);
    const auto sinkPort = coordinatorAddress.substr(colonPos + 1);

    /// StatisticStoreWriter qualifies its output fields with the source name (e.g. "BID$STATISTICID").
    /// The gRPC sink schema must match exactly. The GrpcSink itself uses substring matching on field names,
    /// so it handles the qualifier correctly at runtime.
    const auto qualifier = sourceNameUpper + "$";
    LogicalStatisticFields outputStatisticFields;
    outputStatisticFields.addQualifierName(qualifier);
    Schema grpcSinkSchema;
    grpcSinkSchema.addField(outputStatisticFields.statisticIdField);
    grpcSinkSchema.addField(outputStatisticFields.statisticStartTsField);
    grpcSinkSchema.addField(outputStatisticFields.statisticEndTsField);
    grpcSinkSchema.addField(outputStatisticFields.statisticNumberOfSeenTuplesField);
    /// "host" specifies on which worker to place the gRPC sink. Falls back to the coordinator
    /// host (i.e. the same machine) when not provided, which is correct for single-worker setups.
    const auto hostIt = options.find("host");
    const auto& sinkWorkerHost = hostIt != options.end() ? hostIt->second : sinkHost;
    return LogicalPlanBuilder::addInlineSink(
        "Grpc",
        grpcSinkSchema,
        {{"grpc_host", sinkHost}, {"grpc_port", sinkPort}, {"host", sinkWorkerHost}, {"output_format", "NATIVE"}},
        {},
        plan);
}

/// Append a void sink so the StatisticStoreWriter chain terminates without shipping records out.
/// Used by the WorkloadDomain build branch: the heartbeat probe (a separate query) handles the
/// coordinator reports at a low, configurable rate.
LogicalPlan appendVoidSinkToStatisticChain(LogicalPlan plan, const std::string& sourceNameUpper, const std::unordered_map<std::string, std::string>& options)
{
    const auto qualifier = sourceNameUpper + "$";
    LogicalStatisticFields outputStatisticFields;
    outputStatisticFields.addQualifierName(qualifier);
    Schema voidSinkSchema;
    voidSinkSchema.addField(outputStatisticFields.statisticIdField);
    voidSinkSchema.addField(outputStatisticFields.statisticStartTsField);
    voidSinkSchema.addField(outputStatisticFields.statisticEndTsField);
    voidSinkSchema.addField(outputStatisticFields.statisticNumberOfSeenTuplesField);
    const auto hostIt = options.find("host");
    const auto& sinkWorkerHost = hostIt != options.end() ? hostIt->second : std::string{"localhost:8080"};
    return LogicalPlanBuilder::addInlineSink(
        "Void",
        voidSinkSchema,
        {{"host", sinkWorkerHost}},
        {},
        plan);
}

std::string toUpper(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
    return s;
}

LogicalPlan generateForDataDomain(
    const DataDomain& domain,
    const RequestStatisticBuildStatement& request,
    const Statistic::StatisticId statisticId,
    const std::string& coordinatorAddress)
{
    /// The SQL parser uppercases all unquoted identifiers (via bindIdentifier). Mirror that
    /// here so programmatic callers don't need to think about case.
    const auto sourceNameUpper = toUpper(domain.logicalSourceName);
    const auto fieldNameUpper = toUpper(domain.fieldName);
    auto basePlan = LogicalPlanBuilder::createLogicalPlan(domain.logicalSourceName);
    auto plan = stackBuildChainOnTop(std::move(basePlan), fieldNameUpper, request, statisticId);
    return appendGrpcSinkToStatisticChain(std::move(plan), sourceNameUpper, coordinatorAddress, request.options);
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
                /// generateQuery is used by callers that submit the plan as a standalone query.
                /// WorkloadDomain produces a build branch meant to be spliced into a running data
                /// query (so it has no source on its own); callers must use generateWorkloadBranch
                /// directly with the data query's source operator as the splice leaf.
                throw NotImplemented(
                    "REQUEST STATISTIC WORKLOAD cannot be deployed via the standard generateQuery path. "
                    "The caller must invoke generateWorkloadBranch with the data query's source operator "
                    "(query {}, operator {}) and splice the result into that query's plan via addRootOperators.",
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

LogicalPlan DefaultStatisticQueryGenerator::generateWorkloadBranch(
    const WorkloadDomain& domain,
    const RequestStatisticBuildStatement& request,
    const Statistic::StatisticId statisticId,
    const std::string& coordinatorAddress,
    const LogicalOperator& spliceLeaf) const
{
    /// We require the splice leaf to be the data query's SourceNameLogicalOperator so we can lift
    /// the logical-source name out for the gRPC-sink schema qualifier (the StatisticStoreWriter
    /// prefixes its output fields with "<SOURCE>$"). The splice leaf is also the operator the
    /// build branch will share with the data query's filter chain: after LogicalSourceExpansionRule
    /// rewrites the multi-parent source-name into a Union(SourceDescriptors), both subtrees point
    /// at the same expansion and the runtime fans one source thread out to both pipelines.
    const auto sourceNameOp = spliceLeaf.tryGetAs<SourceNameLogicalOperator>();
    if (not sourceNameOp.has_value())
    {
        throw InvalidConfigParameter(
            "generateWorkloadBranch expects the splice leaf to be a SourceNameLogicalOperator (got operator id {}); "
            "the WorkloadDomain MVP only supports splicing at the data query's source operator.",
            spliceLeaf.getId());
    }
    const auto sourceNameUpper = toUpper((*sourceNameOp)->getLogicalSourceName());
    const auto fieldNameUpper = toUpper(domain.fieldName);
    LogicalPlan basePlan{INVALID_QUERY_ID, {spliceLeaf}};
    auto plan = stackBuildChainOnTop(std::move(basePlan), fieldNameUpper, request, statisticId);
    /// Build branch terminates at VoidSink — the heartbeat probe is responsible for reporting to
    /// the coordinator. Avoids per-window-close gRPC traffic on the data-query source thread.
    (void)coordinatorAddress;
    return appendVoidSinkToStatisticChain(std::move(plan), sourceNameUpper, request.options);
}

LogicalPlan DefaultStatisticQueryGenerator::generateProbeQuery(
    const Statistic::StatisticId statisticId,
    const std::string& coordinatorAddress,
    const uint64_t intervalMs,
    const std::string& sinkWorkerHost) const
{
    PRECONDITION(not coordinatorAddress.empty(), "Required to have a coordinator gRPC address!");
    PRECONDITION(intervalMs > 0, "intervalMs must be > 0");

    /// Schema mirrors the StatisticStoreWriter output / GrpcSink statistic-report schema.
    /// All four columns are UINT64 and unqualified — GrpcSink uses substring matching on field names
    /// so the absence of a SOURCE$ qualifier is fine here.
    Schema probeSchema;
    probeSchema.addField({"STATISTICID", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
    probeSchema.addField({"STATISTICSTART", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
    probeSchema.addField({"STATISTICEND", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
    probeSchema.addField(
        {"STATISTICNUMBEROFSEENTUPLES", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});

    /// Emit the constant tuple (statisticId, 0, 0, 0) at 1 tuple per `intervalMs`. SequenceField
    /// with start==end and step==0 emits the start value forever (sequencePosition never advances
    /// past sequenceEnd; see SequenceField::generate in GeneratorFields.cpp). We set start=N and
    /// end=N+1 so the first emission has pos<end (=>OK), step=0 keeps pos at N, and the post-
    /// emission stop check fails (N < N+1) so the source never stops.
    const auto rawId = statisticId.getRawValue();
    const auto generatorSchema = std::format(
        "SEQUENCE UINT64 {} {} 0, SEQUENCE UINT64 0 1 0, SEQUENCE UINT64 0 1 0, SEQUENCE UINT64 0 1 0", rawId, rawId + 1);
    const auto emitRate = std::max<uint64_t>(1, 1000ULL / intervalMs);
    const auto emitRateConfig = std::format("emit_rate {}", emitRate);

    auto plan = LogicalPlanBuilder::createLogicalPlan(
        "Generator",
        probeSchema,
        {
            {"stop_generator_when_sequence_finishes", "NONE"},
            {"generator_rate_config", emitRateConfig},
            {"flush_interval_ms", std::to_string(intervalMs)},
            {"max_runtime_ms", "100000000"},
            {"seed", std::to_string(rawId)},
            {"generator_schema", generatorSchema},
            {"host", sinkWorkerHost},
        },
        {{"type", "CSV"}});

    const auto colonPos = coordinatorAddress.find(':');
    const auto sinkHost = coordinatorAddress.substr(0, colonPos);
    const auto sinkPort = coordinatorAddress.substr(colonPos + 1);
    plan = LogicalPlanBuilder::addInlineSink(
        "Grpc",
        probeSchema,
        {{"grpc_host", sinkHost}, {"grpc_port", sinkPort}, {"host", sinkWorkerHost}, {"output_format", "NATIVE"}},
        {},
        plan);
    return plan;
}

}
