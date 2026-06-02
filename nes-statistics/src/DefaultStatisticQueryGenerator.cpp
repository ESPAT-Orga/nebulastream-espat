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
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/SketchDimensions.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Traits/SpliceToRunningSourceTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramProbeLogicalOperator.hpp>
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
    const Statistic::StatisticId statisticId,
    const bool applyConditionSelection)
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
    /// Apply the trigger's condition only in the DataDomain build chain (where the build branch
    /// flows to a gRPC sink, so the Selection filters which window-close results get reported).
    /// In the WorkloadDomain path the same `condition` is reinterpreted as a *probe-pipeline*
    /// predicate over histogram bin fields (BINSTART/BINEND/BINCOUNTER) — those aren't present
    /// on the build chain's StatisticStoreWriter output, so applying the Selection here would
    /// fail to bind. The caller passes applyConditionSelection=false for the workload-domain path.
    if (applyConditionSelection and request.conditionTrigger.has_value() and request.conditionTrigger->condition.has_value())
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
    auto plan = stackBuildChainOnTop(std::move(basePlan), fieldNameUpper, request, statisticId, /*applyConditionSelection=*/true);
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

    /// Stamp SpliceToRunningSourceTrait on the build branch's source operator so the worker, on
    /// instantiating this source, splices into the already-running data-query source thread for
    /// the same logical source instead of spawning a second source thread.
    auto taggedSource = spliceLeaf;
    {
        auto ts = taggedSource.getTraitSet();
        [[maybe_unused]] const auto inserted = tryInsert(ts, SpliceToRunningSourceTrait{});
        taggedSource = taggedSource.withTraitSet(ts);
    }

    LogicalPlan basePlan{INVALID_QUERY_ID, {taggedSource}};
    /// applyConditionSelection=false: the trigger's `condition` is a probe-pipeline predicate
    /// (binds against histogram bin fields), not a build-chain output filter.
    auto plan = stackBuildChainOnTop(std::move(basePlan), fieldNameUpper, request, statisticId, /*applyConditionSelection=*/false);

    const auto predicate
        = request.conditionTrigger.has_value() ? request.conditionTrigger->condition : std::optional<LogicalFunction>{};

    if (not predicate.has_value())
    {
        /// No predicate: terminate at VoidSink so the build chain quietly populates the store
        /// without shipping window-close records anywhere. (Heartbeat-style probe is no longer
        /// deployed separately; users wiring a callback without a predicate get no triggers.)
        (void)coordinatorAddress;
        return appendVoidSinkToStatisticChain(std::move(plan), sourceNameUpper, request.options);
    }

    /// Probe-in-build path: instead of VoidSink we chain
    ///   StatisticStoreWriter → EquiWidthHistogramProbe → Selection(predicate) → GrpcSink
    /// The writer emits one record per window-close carrying that window's (statId, startTs,
    /// endTs, seenTuples). The probe reads those values from its input record (not from any
    /// sentinel constant) and fetches that exact window's bins from the store. So:
    ///  - No Generator-driven polling: triggers fire per window-close, at the build's natural
    ///    cadence (~3 Hz here).
    ///  - No latest-window guessing: the probe's lookup key matches the just-written window
    ///    exactly. No stale data, no ambiguity.
    ///  - Selection filters which bin rows make it to the GrpcSink (the bin-level predicate).
    ///  - The GrpcSink reports each surviving bin row to the coordinator; the report's
    ///    STATISTICID is the build's statisticId, which is also the routing key for the
    ///    coordinator-side probe callback registered by collectWorkloadStatistic.
    plan = promoteOperatorToRoot(
        plan,
        EquiWidthHistogramProbeLogicalOperator{
            statisticId,
            DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE),
            DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
    plan = LogicalPlanBuilder::addSelection(*predicate, plan);

    /// GrpcSink schema mirrors the StatisticStoreWriter's output schema (the four
    /// LogicalStatisticFields), qualified with the source name. The probe operator adds bin
    /// fields (BINSTART/BINCOUNTER/BINEND) to its output schema; we drop them via a Projection
    /// so the sink only carries the four reporting fields the coordinator's gRPC service
    /// expects. STATISTICID stays = build's statisticId, so coordinator-side
    /// `probeCallbacks[statisticId]` routes correctly.
    const auto& systemQualifier = sourceNameUpper + "$";
    LogicalStatisticFields outputStatisticFields;
    outputStatisticFields.addQualifierName(systemQualifier);
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticIdField.name},
        LogicalFunction{FieldAccessLogicalFunction{"STATISTICID"}});
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticStartTsField.name},
        LogicalFunction{FieldAccessLogicalFunction{"STATISTICSTART"}});
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticEndTsField.name},
        LogicalFunction{FieldAccessLogicalFunction{"STATISTICEND"}});
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticNumberOfSeenTuplesField.name},
        LogicalFunction{FieldAccessLogicalFunction{"STATISTICNUMBEROFSEENTUPLES"}});
    plan = LogicalPlanBuilder::addProjection(std::move(projections), /*asterisk=*/false, plan);

    return appendGrpcSinkToStatisticChain(std::move(plan), sourceNameUpper, coordinatorAddress, request.options);
}

LogicalPlan DefaultStatisticQueryGenerator::generateProbeQuery(
    const Statistic::StatisticId buildStatisticId,
    const Statistic::StatisticId probeStatisticId,
    std::optional<LogicalFunction> predicate,
    const std::string& coordinatorAddress,
    const uint64_t intervalMs,
    const std::string& sinkWorkerHost) const
{
    PRECONDITION(not coordinatorAddress.empty(), "Required to have a coordinator gRPC address!");
    PRECONDITION(intervalMs > 0, "intervalMs must be > 0");

    /// In the gated mode the Generator drives the EquiWidthHistogramProbe, whose physical reader
    /// looks up the histogram via the input record's STATISTICID. So we set STATISTICID =
    /// buildStatisticId here. In the heartbeat mode (no predicate) there is no histogram lookup —
    /// the Generator value flows straight to GrpcSink, so we use probeStatisticId so the report
    /// already carries the routing key.
    const auto generatorStatId = predicate.has_value() ? buildStatisticId.getRawValue() : probeStatisticId.getRawValue();

    /// Schema for the Generator source's emitted heartbeat records. In the gated mode we need a
    /// distinct first-field qualifier so the system-generated qualifier extracted by
    /// Schema::getQualifierNameForSystemGeneratedFieldsWithSeparator (which splits the first
    /// field name at "$") doesn't collide with a literal "STATISTICID" prefix — that previously
    /// caused the EquiWidthProbe's input-resolved field "STATISTICID" to be rewritten to
    /// "STATISTICID$STATISTICID", which the runtime then failed to find in records. With a
    /// "PROBE$" qualifier here, every downstream field cleanly inherits "PROBE$" until the
    /// final Projection strips the qualifier off again before the GrpcSink sees the row.
    /// In the heartbeat (non-gated) mode the records flow directly to the GrpcSink, whose
    /// substring matching on field names treats "ZZZ$STATISTICID" identically to "STATISTICID".
    Schema probeSchema;
    probeSchema.addField(
        {"ZZZ$STATISTICID", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
    probeSchema.addField(
        {"ZZZ$STATISTICSTART", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
    probeSchema.addField(
        {"ZZZ$STATISTICEND", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
    probeSchema.addField(
        {"ZZZ$STATISTICNUMBEROFSEENTUPLES", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});

    /// Emit the constant tuple (generatorStatId, 0, 0, 0) at 1 tuple per `intervalMs`. SequenceField
    /// with start==end and step==0 emits the start value forever (sequencePosition never advances
    /// past sequenceEnd; see SequenceField::generate in GeneratorFields.cpp). We set start=N and
    /// end=N+1 so the first emission has pos<end (=>OK), step=0 keeps pos at N, and the post-
    /// emission stop check fails (N < N+1) so the source never stops.
    /// STATISTICSTART = 0, STATISTICEND = LATEST_WINDOW_END_SENTINEL (UINT64_MAX - 1) so the
    /// downstream StatisticStoreReader's "latest closed window" fallback kicks in (see
    /// resolveStatistic in StatisticStoreReader.cpp). Each probe tick reads whatever the build
    /// branch most recently wrote — no need to coordinate window timestamps between concurrently
    /// running build and probe pipelines. SequenceField with start = N, end = N+1, step = 0
    /// emits N forever (sequencePosition stays at start; stop-check only fires when pos >= end).
    constexpr uint64_t latestWindowSentinel = std::numeric_limits<uint64_t>::max() - 1;
    const auto generatorSchema = std::format(
        "SEQUENCE UINT64 {} {} 0, SEQUENCE UINT64 0 1 0, SEQUENCE UINT64 {} {} 0, SEQUENCE UINT64 0 1 0",
        generatorStatId,
        generatorStatId + 1,
        latestWindowSentinel,
        latestWindowSentinel + 1);
    /// emit_rate is parsed as double by FixedGeneratorRate, so fractional rates (sub-1 tuple/sec)
    /// are supported. We want one probe tuple every `intervalMs`, i.e. `1000/intervalMs` tup/sec.
    /// Integer arithmetic here previously clamped the rate to 1 tuple/sec for any intervalMs > 1000,
    /// which caused the swap callback to fire too often (e.g. 1× per second for intervalMs=10000).
    const auto emitRate = 1000.0 / static_cast<double>(intervalMs);
    const auto emitRateConfig = std::format("emit_rate {}", emitRate);

    auto plan = LogicalPlanBuilder::createLogicalPlan(
        "Generator",
        probeSchema,
        {
            {"stop_generator_when_sequence_finishes", "NONE"},
            {"generator_rate_config", emitRateConfig},
            {"flush_interval_ms", std::to_string(intervalMs)},
            {"max_runtime_ms", "100000000"},
            {"seed", std::to_string(probeStatisticId.getRawValue())},
            {"generator_schema", generatorSchema},
            {"host", sinkWorkerHost},
        },
        {{"type", "CSV"}});

    /// Schema reported to the GrpcSink. Heartbeat path: matches the Generator's PROBE$-qualified
    /// fields directly. Gated path: matches the Projection's explicitly-unqualified output
    /// (so the GrpcSink's substring-match on STATISTICID etc. works on a clean schema).
    Schema sinkSchema;
    if (predicate.has_value())
    {
        sinkSchema.addField({"STATISTICID", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
        sinkSchema.addField({"STATISTICSTART", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
        sinkSchema.addField({"STATISTICEND", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
        sinkSchema.addField(
            {"STATISTICNUMBEROFSEENTUPLES", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});
    }
    else
    {
        sinkSchema = probeSchema;
    }

    if (predicate.has_value())
    {
        /// Selectivity-gated probe path:
        ///   Generator → EquiWidthHistogramProbe(reads buildStatisticId's histogram)
        ///             → Selection(predicate over bin fields)
        ///             → Projection({STATISTICID := probeStatisticId, pass timestamps + seen-tuples})
        /// The probe operator emits one record per bin; Selection filters bin rows, and the
        /// Projection collapses every surviving row to a 4-field tuple carrying the regime-routing
        /// statisticId. Multiple bins surviving the predicate just fire the same callback more
        /// than once — the swap logic is expected to be idempotent.
        /// counterType=UINT64 (per-bin counters are integers), startEndType=UINT64 to match the
        /// EquiWidthHistogram build's actual bin-start storage layout — even though the underlying
        /// field is FLOAT64, the histogram quantizes to integer-step bins. Mismatched startEnd
        /// type causes the IEEE-float bytes to be reinterpreted as integers in BINSTART/BINEND,
        /// making comparisons like "BINSTART >= 900" silently never match.
        plan = promoteOperatorToRoot(
            plan,
            EquiWidthHistogramProbeLogicalOperator{
                buildStatisticId,
                DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE),
                DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE)});

        plan = LogicalPlanBuilder::addSelection(*predicate, plan);

        /// Overwrite STATISTICID with the probeStatisticId (the routing key the coordinator
        /// matches against) while preserving the timestamps + seen-tuples. Explicit
        /// FieldIdentifiers on the pass-throughs keep the output field names unqualified so the
        /// downstream GrpcSink's declared schema matches. Without the explicit identifiers, the
        /// FieldAccess's inferred name would inherit the system qualifier the EquiWidthProbe
        /// stamped on its outputs (e.g. "STATISTICID$STATISTICSTART"), and the GrpcSink schema
        /// inference would reject the output.
        std::vector<ProjectionLogicalOperator::Projection> projections;
        projections.emplace_back(
            FieldIdentifier{"STATISTICID"},
            LogicalFunction{ConstantValueLogicalFunction{
                DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE),
                std::to_string(probeStatisticId.getRawValue())}});
        projections.emplace_back(FieldIdentifier{"STATISTICSTART"}, LogicalFunction{FieldAccessLogicalFunction{"STATISTICSTART"}});
        projections.emplace_back(FieldIdentifier{"STATISTICEND"}, LogicalFunction{FieldAccessLogicalFunction{"STATISTICEND"}});
        projections.emplace_back(
            FieldIdentifier{"STATISTICNUMBEROFSEENTUPLES"}, LogicalFunction{FieldAccessLogicalFunction{"STATISTICNUMBEROFSEENTUPLES"}});
        plan = LogicalPlanBuilder::addProjection(std::move(projections), /*asterisk=*/false, plan);
    }

    const auto colonPos = coordinatorAddress.find(':');
    const auto sinkHost = coordinatorAddress.substr(0, colonPos);
    const auto sinkPort = coordinatorAddress.substr(colonPos + 1);
    plan = LogicalPlanBuilder::addInlineSink(
        "Grpc",
        sinkSchema,
        {{"grpc_host", sinkHost}, {"grpc_port", sinkPort}, {"host", sinkWorkerHost}, {"output_format", "NATIVE"}},
        {},
        plan);
    return plan;
}

}
