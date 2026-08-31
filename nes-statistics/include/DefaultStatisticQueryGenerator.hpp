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

    explicit DefaultStatisticQueryGenerator(const bool enableHistogramDeltaCompression)
        : enableHistogramDeltaCompression(enableHistogramDeltaCompression)
    {
    }

    [[nodiscard]] LogicalPlan generateQuery(
        const RequestStatisticBuildStatement& request,
        StatisticTuple::StatisticId statisticId,
        const std::string& coordinatorAddress) const override;

    /// TODO: statistic-renaming also overrides generateWorkloadBranch and
    /// generateWorkloadBranchPrometheus, which splice a build branch onto a *running* data query's source
    /// (stamping SpliceToRunningSourceTrait so the worker fans that source out to both pipelines). Those
    /// overrides are not ported: they pull in the SpliceToRunningSource / PlacementHint / PinnedHost traits
    /// and a PrometheusSink, none of which exist upstream, and none of it is needed to collect a statistic
    /// on a source field. We deliberately leave them unoverridden so the base class's NotImplemented applies
    /// -- StatisticQueryGenerator documents that as the supported behaviour for generators without
    /// WorkloadDomain support. StatisticManager::collectWorkloadStatistic is unchanged and still calls
    /// through; it simply surfaces that NotImplemented.

private:
    bool enableHistogramDeltaCompression = false;
};

}
