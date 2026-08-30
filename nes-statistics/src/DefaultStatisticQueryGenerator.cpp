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
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <CollectionDomain.hpp>
#include <RequestStatisticStatement.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

LogicalPlan DefaultStatisticQueryGenerator::generateQuery(
    const RequestStatisticBuildStatement& /*request*/,
    StatisticTuple::StatisticId /*statisticId*/,
    const std::string& /*coordinatorAddress*/) const
{
    throw NotImplemented("DefaultStatisticQueryGenerator::generateQuery: statistic operators not yet ported");
}

LogicalPlan DefaultStatisticQueryGenerator::generateWorkloadBranch(
    const WorkloadDomain& /*domain*/,
    const RequestStatisticBuildStatement& /*request*/,
    StatisticTuple::StatisticId /*statisticId*/,
    const std::string& /*coordinatorAddress*/,
    const LogicalOperator& /*spliceLeaf*/) const
{
    throw NotImplemented("DefaultStatisticQueryGenerator::generateWorkloadBranch: statistic operators not yet ported");
}

LogicalPlan DefaultStatisticQueryGenerator::generateWorkloadBranchPrometheus(
    const WorkloadDomain& /*domain*/,
    const RequestStatisticBuildStatement& /*request*/,
    const LogicalOperator& /*spliceLeaf*/) const
{
    throw NotImplemented("DefaultStatisticQueryGenerator::generateWorkloadBranchPrometheus: statistic operators not yet ported");
}

}
