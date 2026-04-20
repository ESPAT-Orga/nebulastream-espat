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
#include <Plans/LogicalPlan.hpp>
#include <Statistic.hpp>

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
};

}
