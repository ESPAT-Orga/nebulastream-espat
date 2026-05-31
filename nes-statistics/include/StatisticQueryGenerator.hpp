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

#include <cstdint>
#include <optional>
#include <string>
#include <CollectionDomain.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Statistic.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Forward-declared to keep this interface header decoupled from the concrete statement definition.
/// Including RequestStatisticBuildStatement.hpp would pull in:
///   RequestStatisticBuildStatement.hpp --> CollectionDomain.hpp --> Metric.hpp --> Statistic.hpp
/// This header only needs the type for a const reference parameter.
struct RequestStatisticBuildStatement;

/// Abstract interface for generating statistic collection queries.
/// Allows swapping the generator (e.g., for testing with a mock, or a future cost-based generator).
class StatisticQueryGenerator
{
public:
    virtual ~StatisticQueryGenerator() = default;

    /// Generates a LogicalPlan that builds a statistic and writes it to the StatisticStore.
    /// The statisticId is provided by the StatisticCoordinator and uniquely identifies this statistic.
    [[nodiscard]] virtual LogicalPlan generateQuery(
        const RequestStatisticBuildStatement& request, Statistic::StatisticId statisticId, const std::string& coordinatorAddress) const
        = 0;

    /// Builds the "build branch" sub-plan for a WorkloadDomain statistic: a chain rooted at the
    /// gRPC sink stacked on top of `spliceLeaf` (the data query's source operator). The caller
    /// then merges the returned plan's roots into the data query's plan so the optimizer's
    /// LogicalSourceExpansionRule produces a single shared Union(SourceDescriptors). Default impl
    /// throws NotImplemented — generators that don't support WorkloadDomain can leave it that way.
    [[nodiscard]] virtual LogicalPlan generateWorkloadBranch(
        const WorkloadDomain& domain,
        const RequestStatisticBuildStatement& request,
        Statistic::StatisticId statisticId,
        const std::string& coordinatorAddress,
        const LogicalOperator& spliceLeaf) const
    {
        (void)domain;
        (void)request;
        (void)statisticId;
        (void)coordinatorAddress;
        (void)spliceLeaf;
        throw NotImplemented("This StatisticQueryGenerator does not support WorkloadDomain build-branch generation");
    }

    /// Probe used alongside the workload-domain build branch.
    /// Two modes (selected by the `predicate` argument):
    ///  - predicate == nullopt: a pure heartbeat. Generator → GrpcSink reports `buildStatisticId`
    ///    every interval. The coordinator-side trigger callback handles all decision logic.
    ///  - predicate != nullopt: a *selectivity-gated* probe. The pipeline reads the histogram via
    ///    EquiWidthHistogramProbe(buildStatisticId), filters bin rows by `predicate`, and rewrites
    ///    STATISTICID to `probeStatisticId` before reporting. The coordinator routes the report
    ///    via probeStatisticId, so two probes built off the same buildStatisticId fire two distinct
    ///    callbacks.
    /// `probeStatisticId` is the report routing key (must differ between probes when stacking
    /// multiple). When `predicate` is null and the caller wants the legacy single-callback flow,
    /// they pass `probeStatisticId == buildStatisticId`.
    [[nodiscard]] virtual LogicalPlan generateProbeQuery(
        Statistic::StatisticId buildStatisticId,
        Statistic::StatisticId probeStatisticId,
        std::optional<LogicalFunction> predicate,
        const std::string& coordinatorAddress,
        uint64_t intervalMs,
        const std::string& sinkWorkerHost) const
    {
        (void)buildStatisticId;
        (void)probeStatisticId;
        (void)predicate;
        (void)coordinatorAddress;
        (void)intervalMs;
        (void)sinkWorkerHost;
        throw NotImplemented("This StatisticQueryGenerator does not support workload-domain heartbeat probes");
    }
};

}
