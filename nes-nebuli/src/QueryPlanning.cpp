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

#include <QueryPlanning.hpp>

#include <utility>

#include <GlobalOptimizer/BottomUpPlacement.hpp>
#include <GlobalOptimizer/GlobalOptimizer.hpp>
#include <GlobalOptimizer/QueryDecomposition.hpp>

namespace NES
{

PlanStage::DistributedLogicalPlan QueryPlanner::plan(PlanStage::BoundLogicalPlan&& boundPlan) &&
{
    PlanStage::OptimizedLogicalPlan optimizedPlan = GlobalOptimizer::with(context).optimize(std::move(boundPlan));
    PlanStage::PlacedLogicalPlan placedPlan = BottomUpOperatorPlacer::with(context).place(std::move(optimizedPlan));
    return PlanStage::DistributedLogicalPlan{QueryDecomposer::with(context).decompose(std::move(placedPlan))};
}

}
