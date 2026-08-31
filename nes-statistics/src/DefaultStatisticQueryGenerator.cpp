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
#include <Schema/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramDeltaGenLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramDeltaResolverLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Histogram/EquiWidthHistogramProbeLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Traits/PinnedHostTrait.hpp>
#include <Traits/PlacementHintTrait.hpp>
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
#include <StatisticTuple.hpp>

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

StatisticTuple::StatisticType toStatisticType(const Metric& metric)
{
    /// For now, we perform a simple mapping of metric to statistic type.
    switch (metric)
    {
        case Metric::Cardinality:
        case Metric::Rate:
            return StatisticTuple::StatisticType::Count_Min_Sketch;
        case Metric::MinVal:
        case Metric::MaxVal:
        case Metric::Selectivity:
            return StatisticTuple::StatisticType::Equi_Width_Histogram;
        case Metric::Average:
            return StatisticTuple::StatisticType::Reservoir_Sample;
    }
    std::unreachable();
}

std::shared_ptr<WindowAggregationLogicalFunction> createAggregationFunction(
    const FieldAccessLogicalFunction& onField,
    const Metric metric,
    const StatisticTuple::StatisticId statisticId,
    const std::unordered_map<std::string, std::string>& options)
{
    switch (toStatisticType(metric))
    {
        case StatisticTuple::StatisticType::Equi_Width_Histogram: {
            const auto memoryBudget = getOption(options, "memory_budget", 4096);
            const auto minValue = getOption(options, "min", 0);
            const auto maxValue = getOption(options, "max", 1000);
            return std::make_shared<WindowAggregationLogicalFunction>(
                EquiWidthHistogramLogicalFunction{onField, memoryBudget, minValue, maxValue, statisticId});
        }
        case StatisticTuple::StatisticType::Reservoir_Sample: {
            const auto memoryBudget = getOption(options, "memory_budget", 8192);
            return std::make_shared<WindowAggregationLogicalFunction>(
                ReservoirSampleLogicalFunction{onField, std::vector{onField}, memoryBudget, statisticId});
        }
        case StatisticTuple::StatisticType::Count_Min_Sketch: {
            const auto memoryBudget = getOption(options, "memory_budget", 8192);
            return std::make_shared<WindowAggregationLogicalFunction>(CountMinSketchLogicalFunction{onField, memoryBudget, statisticId});
        }
        case StatisticTuple::StatisticType::Count:
        case StatisticTuple::StatisticType::Sum:
        case StatisticTuple::StatisticType::Avg:
            throw NotImplemented("Scalar statistics (Count/Sum/Avg) are not produced by the metric-based query generator");
    }
    std::unreachable();
}

bool wantsZstdCompression(const std::unordered_map<std::string, std::string>& options)
{
    const auto it = options.find("compress_statistic");
    return it != options.end() and toLowerCase(it->second) == "zstd";
}

/// TODO: statistic-renaming wraps the synopsis blob in a ZstdCompress / ZstdDecompress logical-function pair
/// around the build, so the payload crosses the network compressed. Those functions (~700 lines across
/// nes-logical-operators and nes-physical-operators, plus their physical counterparts and a systest) are an
/// option-gated feature orthogonal to the statistics themselves, and are deliberately not ported here.
///
/// We reject `compress_statistic=zstd` rather than ignoring it. Silently dropping the stage would hand back a
/// plan whose payload is uncompressed while the caller believes it asked for compression -- and on the probe
/// side that same blob is read back through the matching decompress stage, so the two halves would disagree
/// about the payload's encoding. Failing at plan generation is the honest outcome until the pair is ported.
[[noreturn]] LogicalPlan appendZstdStage(LogicalPlan, StatisticTuple::StatisticId, bool)
{
    throw NotImplemented(
        "compress_statistic=zstd is not supported on this branch: the ZstdCompress/ZstdDecompress logical "
        "functions are not ported yet");
}

/// TODO: statistic-renaming splits an EquiWidthHistogram build into a DeltaGen stage near the source and a
/// DeltaResolver stage near the sink, so only per-window deltas cross the network. That chain needs the
/// EquiWidthHistogramDeltaGen / DeltaResolver logical functions and PlacementHintTrait, none of which are
/// ported. The `enableHistogramDeltaCompression` flag and its constructor are kept so the option survives,
/// but the path is refused rather than silently degraded to a plain (uncompressed, unsplit) histogram build.
[[noreturn]] LogicalPlan appendHistogramDeltaBuildChain(
    LogicalPlan,
    const FieldAccessLogicalFunction&,
    const std::shared_ptr<Windowing::WindowType>&,
    uint64_t,
    const std::unordered_map<std::string, std::string>&,
    StatisticTuple::StatisticId)
{
    throw NotImplemented(
        "histogram delta compression is not supported on this branch: the EquiWidthHistogramDeltaGen and "
        "DeltaResolver aggregation functions are not ported yet");
}

LogicalPlan generateForDataDomain(
    const DataDomain& domain,
    const RequestStatisticBuildStatement& request,
    const StatisticTuple::StatisticId statisticId,
    const std::string& coordinatorAddress,
    const bool enableHistogramDeltaCompression)
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

    auto plan = LogicalPlanBuilder::createLogicalPlan(domain.logicalSourceName);

    const bool useDeltaSplit
        = enableHistogramDeltaCompression && toStatisticType(request.metric) == StatisticTuple::StatisticType::Equi_Width_Histogram;
    if (useDeltaSplit)
    {
        plan = appendHistogramDeltaBuildChain(std::move(plan), onField, windowType, request.windowSizeMs, request.options, statisticId);
    }
    else
    {
        auto agg = createAggregationFunction(onField, request.metric, statisticId, request.options);
        /// The build and statistic store writer need to have a connection for the statistic fields, e.g., statisticDataField.
        /// As the field names change during type inference
        const auto logicalStatisticFields = std::make_shared<LogicalStatisticFields>();
        plan = LogicalPlanBuilder::addStatisticBuild(std::move(plan), windowType, {agg}, {}, logicalStatisticFields);
        if (wantsZstdCompression(request.options))
        {
            plan = appendZstdStage(std::move(plan), statisticId, /*compress=*/true);
            plan = appendZstdStage(std::move(plan), statisticId, /*compress=*/false);
        }
        plan = LogicalPlanBuilder::addStatisticStoreWriter(plan, logicalStatisticFields, statisticId, toStatisticType(request.metric));
    }

    /// Optional `writer_host` SET option: pin the StatisticStoreWriter to a specific worker. The build
    /// stays near the source (leaf) via the placement distance objective; only the writer is forced
    /// onto the named host (e.g. the root), so the per-window synopsis crosses the network to the
    /// writer. Without the option the writer falls to the leaf and only the small terminal-sink record
    /// crosses. Enforced by addStatisticWriterPinningConstraints in BottomUpPlacement.
    if (const auto writerHostIt = request.options.find("writer_host"); writerHostIt != request.options.end())
    {
        /// A second hard pin on the writer the delta split already anchors to Sink; a differing host
        /// would make the placement ILP infeasible, reported only as a generic capacity failure.
        if (useDeltaSplit)
        {
            throw InvalidConfigParameter(
                "'writer_host' cannot be combined with histogram delta compression: the reconstructed histogram is written on the sink "
                "node, next to the RESOLVER that produces it. Use the 'host' option to choose that node.");
        }
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

    /// Terminate the plan. Default "grpc" reports the statistic to a StatisticManager listening at
    /// coordinatorAddress (the REPL path). "void" pins a discarding sink on coordinatorAddress instead:
    /// a plain worker cannot receive Grpc reports (only StatisticManagerService can), so distributed
    /// deployments that only need the writer to run on a target node use the void terminal. Either way
    /// the terminal sink is placed on coordinatorAddress (root), so the projected report record crosses
    /// the network to it.
    ///
    /// An explicit `host` SET option wins over that default: the coordinator's gRPC address is not
    /// necessarily a worker in the topology (in the embedded REPL it is an ephemeral port), and
    /// placing the sink there fails validation with "placed on non-existing worker".
    auto sinkOptions = request.options;
    if (not sinkOptions.contains("host"))
    {
        sinkOptions["host"] = coordinatorAddress;
    }
    if (const auto it = request.options.find("terminal_sink"); it != request.options.end() && it->second == "void")
    {
        return appendWorkloadVoidSink(std::move(plan), sourceNameUpper, sinkOptions);
    }
    return appendWorkloadGrpcSink(std::move(plan), sourceNameUpper, coordinatorAddress, sinkOptions);
}

}

LogicalPlan DefaultStatisticQueryGenerator::generateQuery(
    const RequestStatisticBuildStatement& request, const StatisticTuple::StatisticId statisticId, const std::string& coordinatorAddress) const
{
    return std::visit(
        [&]<typename CollectionDomain>(const CollectionDomain& domain) -> LogicalPlan
        {
            using DomainType = std::decay_t<CollectionDomain>;
            if constexpr (std::is_same_v<DomainType, DataDomain>)
            {
                return generateForDataDomain(domain, request, statisticId, coordinatorAddress, enableHistogramDeltaCompression);
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


}
