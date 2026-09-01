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
#include <memory>
#include <string>
#include <utility>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <StatisticStore/StatisticStoreRegistry.hpp>

namespace NES
{

/// Holds the statistic store for the operators that touch it.
///
/// This is the only route from JIT'd Nautilus code to a C++ shared_ptr: a physical operator reaches its handler
/// through ExecutionContext::getGlobalOperatorHandler, and nothing else in a running pipeline carries the store.
///
/// The store is resolved from the process-global StatisticStoreRegistry by name rather than injected. A lowering
/// rule builds its physical operators from nothing but a logical operator and a trait set, so there is nowhere to
/// inject from -- the same gap InProcessFeedRegistry closes for sources. Resolving here keeps
/// AbstractLoweringRule::apply(LogicalOperator) and every existing lowering rule untouched.
class StatisticStoreOperatorHandler final : public OperatorHandler
{
public:
    /// Resolves (creating if absent) the named store from the registry, so writer and reader agree without either
    /// being constructed first.
    explicit StatisticStoreOperatorHandler(const std::string& storeName = std::string{StatisticStoreRegistry::DEFAULT_STORE_NAME})
        : statisticStore(StatisticStoreRegistry::instance().getOrCreate(storeName))
    {
    }

    /// Test seam: bind a store directly, bypassing the registry.
    explicit StatisticStoreOperatorHandler(std::shared_ptr<AbstractStatisticStore> statisticStore)
        : statisticStore(std::move(statisticStore))
    {
    }

    void start(PipelineExecutionContext&, uint32_t) override { }

    void stop(QueryTerminationType, PipelineExecutionContext&) override { }

    [[nodiscard]] std::shared_ptr<AbstractStatisticStore> getStatisticStore() const { return statisticStore; }

private:
    std::shared_ptr<AbstractStatisticStore> statisticStore;
};

}
