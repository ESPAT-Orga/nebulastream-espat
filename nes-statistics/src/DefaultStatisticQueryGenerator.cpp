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
#include <memory>
#include <optional>
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
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/SketchDimensions.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramProbeLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Traits/PinnedHostTrait.hpp>
#include <Traits/SpliceToRunningSourceTrait.hpp>
#include <Traits/TraitSet.hpp>
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
        case Metric::Selectivity:
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
        case Statistic::StatisticType::Count:
        case Statistic::StatisticType::Sum:
        case Statistic::StatisticType::Avg:
            throw NotImplemented("Scalar statistics (Count/Sum/Avg) are not produced by the metric-based query generator");
    }
    std::unreachable();
}

/// Defined further down in this anonymous namespace; forward-declared so generateForDataDomain can
/// reuse the same terminal-sink builders and uppercaser the workload-domain path uses.
std::string toUpper(std::string s);
LogicalPlan appendWorkloadGrpcSink(
    LogicalPlan plan,
    const std::string& sourceNameUpper,
    const std::string& coordinatorAddress,
    const std::unordered_map<std::string, std::string>& options);
LogicalPlan
appendWorkloadVoidSink(LogicalPlan plan, const std::string& sourceNameUpper, const std::unordered_map<std::string, std::string>& options);

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

    /// Optional `writer_host` SET option: pin the StatisticStoreWriter to a specific worker. The build
    /// stays near the source (leaf) via the placement distance objective; only the writer is forced
    /// onto the named host (e.g. the root), so the per-window synopsis crosses the network to the
    /// writer. Without the option the writer falls to the leaf and only the small terminal-sink record
    /// crosses. Enforced by addStatisticWriterPinningConstraints in BottomUpPlacement.
    if (const auto writerHostIt = request.options.find("writer_host"); writerHostIt != request.options.end())
    {
        auto writer = plan.getRootOperators().front();
        auto ts = writer.getTraitSet();
        [[maybe_unused]] const auto inserted = tryInsert(ts, PinnedHostTrait{Host{writerHostIt->second}});
        plan = plan.withRootOperators({writer.withTraitSet(ts)});
    }

    if (request.conditionTrigger.has_value())
    {
        plan = LogicalPlanBuilder::addSelection(request.conditionTrigger->condition.value(), plan);
    }

    /// Project away the VARSIZED synopsis the StatisticStoreWriter passes through, keeping only the four
    /// scalar report fields. This is what makes the `local` variant cheap: with the writer on the leaf,
    /// only these scalars cross to the root sink. In the `split` variant the synopsis already crossed
    /// upstream (build -> writer over the network), so dropping it here costs nothing.
    const auto sourceNameUpper = toUpper(domain.logicalSourceName);
    LogicalStatisticFields outputStatisticFields;
    outputStatisticFields.addQualifierName(sourceNameUpper + "$");
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticIdField.name}, LogicalFunction{FieldAccessLogicalFunction{"STATISTICID"}});
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticStartTsField.name}, LogicalFunction{FieldAccessLogicalFunction{"STATISTICSTART"}});
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticEndTsField.name}, LogicalFunction{FieldAccessLogicalFunction{"STATISTICEND"}});
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticNumberOfSeenTuplesField.name},
        LogicalFunction{FieldAccessLogicalFunction{"STATISTICNUMBEROFSEENTUPLES"}});
    plan = LogicalPlanBuilder::addProjection(std::move(projections), /*asterisk=*/false, plan);

    /// Terminate the plan. Default "grpc" reports the statistic to a StatisticCoordinator listening at
    /// coordinatorAddress (the REPL path). "void" pins a discarding sink on coordinatorAddress instead:
    /// a plain worker cannot receive Grpc reports (only StatisticCoordinatorService can), so distributed
    /// deployments that only need the writer to run on a target node use the void terminal. Either way
    /// the terminal sink is placed on coordinatorAddress (root), so the projected report record crosses
    /// the network to it.
    auto sinkOptions = request.options;
    sinkOptions["host"] = coordinatorAddress;
    if (const auto it = request.options.find("terminal_sink"); it != request.options.end() && it->second == "void")
    {
        return appendWorkloadVoidSink(std::move(plan), sourceNameUpper, sinkOptions);
    }
    return appendWorkloadGrpcSink(std::move(plan), sourceNameUpper, coordinatorAddress, sinkOptions);
}

/// Build the windowed-aggregation + store-writer chain on top of `basePlan`, stopping before
/// any sink is attached. The workload-domain caller appends either Probe + Selection + Projection
/// + GrpcSink (predicate present — per-window-close the just-written histogram is read back,
/// filtered, and surviving bins ship to the coordinator) or VoidSink (no predicate — the chain
/// quietly populates the store without firing reports).
LogicalPlan stackWorkloadBuildChainOnTop(
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
    /// The trigger's `condition` is intentionally NOT applied here: in the workload-domain path
    /// it is a probe-pipeline predicate (binds against histogram bin fields BINSTART/BINEND/
    /// BINCOUNTER) and gets added downstream of the histogram probe, not on the build chain's
    /// StatisticStoreWriter output.
    return plan;
}

/// Append a gRPC sink terminating the workload-domain build/probe chain. Schema mirrors the
/// StatisticStoreWriter's output (the four LogicalStatisticFields), qualified with the source
/// name so the runtime can resolve the qualified field names on the wire.
LogicalPlan appendWorkloadGrpcSink(
    LogicalPlan plan,
    const std::string& sourceNameUpper,
    const std::string& coordinatorAddress,
    const std::unordered_map<std::string, std::string>& options)
{
    PRECONDITION(not coordinatorAddress.empty(), "Required to have a coordinator gRPC address!");
    const auto colonPos = coordinatorAddress.find(':');
    const auto sinkHost = coordinatorAddress.substr(0, colonPos);
    const auto sinkPort = coordinatorAddress.substr(colonPos + 1);

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

/// Append a void sink terminating the workload-domain build chain when no predicate is attached.
/// The chain quietly populates the store without firing per-window-close reports.
LogicalPlan
appendWorkloadVoidSink(LogicalPlan plan, const std::string& sourceNameUpper, const std::unordered_map<std::string, std::string>& options)
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
    return LogicalPlanBuilder::addInlineSink("Void", voidSinkSchema, {{"host", sinkWorkerHost}}, {}, plan);
}

std::string toUpper(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
    return s;
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
    auto plan = stackWorkloadBuildChainOnTop(std::move(basePlan), fieldNameUpper, request, statisticId);

    const auto predicate = request.conditionTrigger.has_value() ? request.conditionTrigger->condition : std::optional<LogicalFunction>{};

    if (not predicate.has_value())
    {
        /// No predicate: terminate at VoidSink so the build chain quietly populates the store
        /// without shipping window-close records anywhere. (Heartbeat-style probe is no longer
        /// deployed separately; users wiring a callback without a predicate get no triggers.)
        (void)coordinatorAddress;
        return appendWorkloadVoidSink(std::move(plan), sourceNameUpper, request.options);
    }

    /// Probe-in-build path: instead of VoidSink we chain
    ///   StatisticStoreWriter → EquiWidthHistogramProbe → Selection(predicate) → GrpcSink
    /// The writer does two things per window-close:
    ///  (1) Side effect: drops the histogram blob into the in-memory AbstractStatisticStore.
    ///  (2) Pipeline output: emits a 4-field record (statId, startTs, endTs, seenTuples) — just
    ///      the lookup key, not the histogram itself.
    /// The probe receives that key record as input and uses (statId, startTs, endTs) to fetch
    /// the just-written histogram back from the store. At lowering time, EquiWidthHistogramProbe
    /// becomes a StatisticStoreReader physical operator (see LowerToPhysicalEquiWidthHistogramProbe),
    /// so the "reader" exists at runtime even though no separate logical operator appears in this
    /// chain. The reader emits one record per histogram bin (key fields + BINSTART/BINEND/BINCOUNTER).
    /// So:
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
        FieldIdentifier{outputStatisticFields.statisticIdField.name}, LogicalFunction{FieldAccessLogicalFunction{"STATISTICID"}});
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticStartTsField.name}, LogicalFunction{FieldAccessLogicalFunction{"STATISTICSTART"}});
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticEndTsField.name}, LogicalFunction{FieldAccessLogicalFunction{"STATISTICEND"}});
    projections.emplace_back(
        FieldIdentifier{outputStatisticFields.statisticNumberOfSeenTuplesField.name},
        LogicalFunction{FieldAccessLogicalFunction{"STATISTICNUMBEROFSEENTUPLES"}});
    plan = LogicalPlanBuilder::addProjection(std::move(projections), /*asterisk=*/false, plan);

    return appendWorkloadGrpcSink(std::move(plan), sourceNameUpper, coordinatorAddress, request.options);
}

LogicalPlan DefaultStatisticQueryGenerator::generateWorkloadBranchPrometheus(
    const WorkloadDomain& domain, const RequestStatisticBuildStatement& request, const LogicalOperator& spliceLeaf) const
{
    /// Same splice contract as generateWorkloadBranch: the leaf must be the data query's
    /// SourceNameLogicalOperator so the build branch shares one source thread with the data query
    /// (SpliceToRunningSourceTrait → the worker fans the running source out to both pipelines
    /// instead of spawning a second source thread).
    const auto sourceNameOp = spliceLeaf.tryGetAs<SourceNameLogicalOperator>();
    if (not sourceNameOp.has_value())
    {
        throw InvalidConfigParameter(
            "generateWorkloadBranchPrometheus expects the splice leaf to be a SourceNameLogicalOperator (got operator id {}); "
            "the WorkloadDomain MVP only supports splicing at the data query's source operator.",
            spliceLeaf.getId());
    }
    const auto fieldNameUpper = toUpper(domain.fieldName);

    /// server_url is the host:port this sink's Prometheus exposer binds its /metrics endpoint to;
    /// the external Prometheus instance scrapes it. It must be unique per sink, so we require it
    /// explicitly rather than defaulting (a silent default would collide across fields/sources).
    const auto serverUrlIt = request.options.find("prometheus_server_url");
    if (serverUrlIt == request.options.end() or serverUrlIt->second.empty())
    {
        throw InvalidConfigParameter("generateWorkloadBranchPrometheus requires a 'prometheus_server_url' option "
                                     "(the host:port the sink's Prometheus exposer binds to).");
    }
    const auto& serverUrl = serverUrlIt->second;

    /// Tag the shared source so the worker splices it into the already-running data-query source.
    auto taggedSource = spliceLeaf;
    {
        auto ts = taggedSource.getTraitSet();
        [[maybe_unused]] const auto inserted = tryInsert(ts, SpliceToRunningSourceTrait{});
        taggedSource = taggedSource.withTraitSet(ts);
    }

    /// Baseline branch: Source → Projection(field) → PrometheusSink. No in-engine
    /// StatisticBuild/StoreWriter/Probe — the PrometheusSink builds the histogram itself (one
    /// Observe() per tuple), the external Prometheus scrapes the cumulative bucket counters, and
    /// windowing happens at query time via PromQL rate(). We project down to the single monitored
    /// field so the sink builds exactly one histogram, matching the native path's single-field
    /// EquiWidthHistogram for an apples-to-apples per-tuple cost.
    LogicalPlan plan{INVALID_QUERY_ID, {taggedSource}};
    std::vector<ProjectionLogicalOperator::Projection> projections;
    projections.emplace_back(FieldIdentifier{fieldNameUpper}, LogicalFunction{FieldAccessLogicalFunction{fieldNameUpper}});
    plan = LogicalPlanBuilder::addProjection(std::move(projections), /*asterisk=*/false, plan);

    /// Pass an EMPTY schema: SinkLogicalOperator::withInferredSchema fills an inline sink's empty
    /// schema from its input (the projection's single-field output), so we don't need the field's
    /// concrete type here — it isn't resolved on the splice leaf until the optimizer's
    /// type-inference phase runs on submit.
    const auto getOpt = [&](const std::string& key, const std::string& dflt)
    {
        const auto it = request.options.find(key);
        return it != request.options.end() ? it->second : dflt;
    };
    /// min/max default to the PrometheusSink's own equi-width bounds; "min"/"max" are the same
    /// option keys the native EquiWidthHistogram reads (createAggregationFunction), so a single
    /// --companion-histogram-min/max pair configures both paths identically.
    std::unordered_map<std::string, std::string> sinkConfig{
        {"server_url", serverUrl},
        {"histogram_num_buckets", getOpt("histogram_num_buckets", "100")},
        {"histogram_min_value", getOpt("min", "0")},
        {"histogram_max_value", getOpt("max", "1000000")},
        {"output_format", "NATIVE"},
        {"host", getOpt("host", "localhost:8080")}};
    return LogicalPlanBuilder::addInlineSink("Prometheus", Schema{}, std::move(sinkConfig), {}, plan);
}

}
