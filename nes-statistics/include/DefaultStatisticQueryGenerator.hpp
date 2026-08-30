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

#include <string>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <CollectionDomain.hpp>
#include <RequestStatisticStatement.hpp>
#include <StatisticTuple.hpp>
#include <StatisticQueryGenerator.hpp>

namespace NES
{

/// Default implementation that maps Metric → StatisticType and generates a plan using LogicalPlanBuilder.
/// Plan structure: Source → StatisticBuild → StatisticStoreWriter → GrpcSink (if coordinator address is set)
class DefaultStatisticQueryGenerator : public StatisticQueryGenerator
{
public:
    DefaultStatisticQueryGenerator() = default;

    [[nodiscard]] LogicalPlan generateQuery(
        const RequestStatisticBuildStatement& request,
        StatisticTuple::StatisticId statisticId,
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
        StatisticTuple::StatisticId statisticId,
        const std::string& coordinatorAddress,
        const LogicalOperator& spliceLeaf) const override;

    /// Prometheus-baseline build branch: Source → Projection(field) → PrometheusSink. See the base
    /// interface declaration for the contract. The sink is given an empty schema; type inference
    /// fills it from the projection's single-field output (the field type isn't resolved on the
    /// splice leaf until the optimizer runs).
    [[nodiscard]] LogicalPlan generateWorkloadBranchPrometheus(
        const WorkloadDomain& domain, const RequestStatisticBuildStatement& request, const LogicalOperator& spliceLeaf) const override;

};

}
