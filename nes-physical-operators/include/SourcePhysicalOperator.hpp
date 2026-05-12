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

#include <memory>
#include <optional>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
class SourcePhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit SourcePhysicalOperator(SourceDescriptor descriptor, OriginId id);
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

    [[nodiscard]] SourceDescriptor getDescriptor() const;
    [[nodiscard]] OriginId getOriginId() const;

    /// True when the lowering rule found SpliceToRunningSourceTrait on the logical source operator.
    /// The compiled query plan / runtime use this to skip source-thread creation and graft this
    /// query's pipelines onto the already-running source for the matching logical name.
    bool spliceToRunningSource = false;
    /// True when the lowering rule found DeferSourceStartTrait. The runtime registers the source
    /// but does NOT start its emit thread until `deferStartExpectedSpliceCount` successful
    /// appendSuccessors() calls have happened. Used by collectWorkloadStatistic so N splices can
    /// wire in before the source emits sequence 0.
    bool deferStart = false;
    uint32_t deferStartExpectedSpliceCount = 1;
    /// Logical source name resolved at lowering time, pinned so the runtime splice lookup does
    /// not depend on the descriptor still being live.
    std::string logicalSourceName;

    bool operator==(const SourcePhysicalOperator& other) const;

private:
    std::optional<PhysicalOperator> child;
    OriginId originId;
    SourceDescriptor descriptor;
};
}
