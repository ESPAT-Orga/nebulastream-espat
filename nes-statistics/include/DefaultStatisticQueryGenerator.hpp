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

#pragma once

#include <optional>
#include <string>
#include <CollectionDomain.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <RequestStatisticStatement.hpp>
#include <Statistic.hpp>
#include <StatisticQueryGenerator.hpp>

namespace NES
{

/// Default implementation that maps Metric → StatisticType and generates a plan using LogicalPlanBuilder.
/// Plan structure: Source → StatisticBuild → StatisticStoreWriter → GrpcSink (if coordinator address is set)
class DefaultStatisticQueryGenerator : public StatisticQueryGenerator
{
public:
    [[nodiscard]] LogicalPlan generateQuery(
        const RequestStatisticBuildStatement& request,
        Statistic::StatisticId statisticId,
        const std::string& coordinatorAddress) const override;

    /// Builds the "build branch" sub-plan for a WorkloadDomain statistic: a chain rooted at the
    /// gRPC sink with WatermarkAssign → StatisticBuild → StatisticStoreWriter → GrpcSink stacked
    /// on top of `spliceLeaf`. The caller passes the data query's source operator (a
    /// SourceNameLogicalOperator) as `spliceLeaf`; the returned plan can then be merged into the
    /// data query via addRootOperators, so the LogicalSourceExpansionRule produces a single
    /// Union(SourceDescriptors) shared by both the data query's filter chain and the build branch.
    [[nodiscard]] LogicalPlan generateWorkloadBranch(
        const WorkloadDomain& domain,
        const RequestStatisticBuildStatement& request,
        Statistic::StatisticId statisticId,
        const std::string& coordinatorAddress,
        const LogicalOperator& spliceLeaf) const override;

    /// Probe for the workload-domain build branch. See base-class docs for predicate modes.
    /// When predicate is null: legacy ticker — Generator(STATISTICID = probeStatisticId, …) → GrpcSink.
    /// When predicate is set: selectivity-gated — Generator(STATISTICID = buildStatisticId, …) →
    ///   EquiWidthHistogramProbe(buildStatisticId) → Selection(predicate) →
    ///   Projection(STATISTICID := probeStatisticId, pass-through timestamps) → GrpcSink.
    [[nodiscard]] LogicalPlan generateProbeQuery(
        Statistic::StatisticId buildStatisticId,
        Statistic::StatisticId probeStatisticId,
        std::optional<LogicalFunction> predicate,
        const std::string& coordinatorAddress,
        uint64_t intervalMs,
        const std::string& sinkWorkerHost) const override;
};

}
