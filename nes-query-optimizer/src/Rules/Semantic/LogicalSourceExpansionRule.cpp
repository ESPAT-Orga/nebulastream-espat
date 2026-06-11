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

#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/SourceInferenceRule.hpp>
#include <Traits/DeferSourceStartTrait.hpp>
#include <Traits/SpliceToRunningSourceTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

const std::type_info& LogicalSourceExpansionRule::getType()
{
    return typeid(LogicalSourceExpansionRule);
}

std::string_view LogicalSourceExpansionRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> LogicalSourceExpansionRule::dependsOn() const
{
    return {typeid(SourceInferenceRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> LogicalSourceExpansionRule::requiredBy() const
{
    return {};
};

bool LogicalSourceExpansionRule::operator==(const LogicalSourceExpansionRule& other) const
{
    return sourceCatalog == other.sourceCatalog;
}

LogicalPlan LogicalSourceExpansionRule::apply(LogicalPlan queryPlan) const
{
    /// A SourceNameLogicalOperator reachable from multiple root operators (e.g. a workload-domain
    /// splice where a stat-build subtree shares the data-query's source) appears multiple times in
    /// the BFS getOperatorByType traversal. Deduplicate by OperatorId so we expand each unique
    /// source once and let replaceSubtree update every occurrence in the plan in one pass.
    std::unordered_set<OperatorId> processed;
    for (const auto& sourceOp : getOperatorByType<SourceNameLogicalOperator>(queryPlan))
    {
        if (not processed.insert(sourceOp.getId()).second)
        {
            continue;
        }

        const auto logicalSourceOpt = sourceCatalog->getLogicalSource(sourceOp->getLogicalSourceName());
        if (not logicalSourceOpt.has_value())
        {
            throw UnknownSourceName("{}", sourceOp->getLogicalSourceName());
        }
        const auto& logicalSource = logicalSourceOpt.value();
        const auto entriesOpt = sourceCatalog->getPhysicalSources(logicalSource);

        if (not entriesOpt.has_value())
        {
            throw UnknownSourceName("Source \"{}\" was removed concurrently", sourceOp->getLogicalSourceName());
        }
        const auto& entries = entriesOpt.value();
        if (entries.empty())
        {
            throw UnknownSourceName("No physical sources present for logical source \"{}\"", sourceOp->getLogicalSourceName());
        }

        /// Preserve SpliceToRunningSourceTrait / DeferSourceStartTrait across expansion: if the
        /// source-name op was tagged, every expanded SourceDescriptor must carry the same marker
        /// (and the DeferSourceStartTrait's expectedSpliceCount payload) so the runtime hooks
        /// can recognize it at instantiation time.
        const bool spliceMarker = hasTrait<SpliceToRunningSourceTrait>(sourceOp.getTraitSet());
        const auto deferStartTrait = sourceOp.getTraitSet().tryGet<DeferSourceStartTrait>();
        auto expandedSourceOperators = entries
            | std::views::transform(
                                           [spliceMarker, &deferStartTrait](const auto& entry)
                                           {
                                               LogicalOperator op{SourceDescriptorLogicalOperator{entry}};
                                               auto ts = op.getTraitSet();
                                               if (spliceMarker)
                                               {
                                                   [[maybe_unused]] const auto inserted = tryInsert(ts, SpliceToRunningSourceTrait{});
                                               }
                                               if (deferStartTrait.has_value())
                                               {
                                                   [[maybe_unused]] const auto inserted = tryInsert(ts, deferStartTrait.value().get());
                                               }
                                               if (spliceMarker or deferStartTrait.has_value())
                                               {
                                                   op = op.withTraitSet(ts);
                                               }
                                               return op;
                                           })
            | std::ranges::to<std::vector>();

        /// Replace the source-name op (rather than its parent) with the Union(SourceDescriptors)
        /// subtree. This handles both single-parent (the historical assumption) and multi-parent
        /// DAG-shaped plans uniformly: replaceSubtree by id substitutes every occurrence in the
        /// plan, so all parents converge on the same expanded subtree without further bookkeeping.
        const auto unionWithExpansion = LogicalOperator{UnionLogicalOperator{}.withChildren(std::move(expandedSourceOperators))};
        auto replaceResult = replaceSubtree(queryPlan, sourceOp.getId(), unionWithExpansion);

        INVARIANT(
            replaceResult.has_value(),
            "Failed to replace SourceNameLogicalOperator {} with expansion {}",
            sourceOp.explain(ExplainVerbosity::Debug),
            unionWithExpansion.explain(ExplainVerbosity::Debug));
        queryPlan = std::move(replaceResult.value());
    }
    return queryPlan;
}

}
