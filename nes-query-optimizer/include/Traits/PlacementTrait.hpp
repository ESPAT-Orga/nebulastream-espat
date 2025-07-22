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
#include <typeinfo>

#include <Identifiers/Identifiers.hpp>
<<<<<<<< HEAD:nes-nebuli/include/QueryManager/QueryManager.hpp
#include <Listeners/QueryLog.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
class LogicalPlan;
}

namespace NES
{

class QueryManager
{
public:
    virtual ~QueryManager() = default;
    [[nodiscard]] virtual std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan) = 0;
    virtual std::expected<void, Exception> start(QueryId queryId) noexcept = 0;
    virtual std::expected<void, Exception> stop(QueryId queryId) noexcept = 0;
    virtual std::expected<void, Exception> unregister(QueryId queryId) noexcept = 0;
    [[nodiscard]] virtual std::expected<QuerySummary, Exception> status(QueryId queryId) const noexcept = 0;
========
#include <Traits/Trait.hpp>
#include <SerializableTrait.pb.h>

namespace NES
{

/// Assigns a placement on a physical node to an operator
struct PlacementTrait final : TraitConcept
{
    explicit PlacementTrait(std::string nodeId);

    std::string onNode;

    bool operator==(const TraitConcept& other) const override;
    [[nodiscard]] const std::type_info& getType() const override;
    [[nodiscard]] SerializableTrait serialize() const override;
>>>>>>>> fc74d9f568 (feat(Distributed): adds distributed query planning):nes-query-optimizer/include/Traits/PlacementTrait.hpp
};

}
