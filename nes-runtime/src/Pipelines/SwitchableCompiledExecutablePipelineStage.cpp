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
#include <Pipelines/SwitchableCompiledExecutablePipelineStage.hpp>

#include <atomic>
#include <memory>
#include <ostream>
#include <utility>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

SwitchableCompiledExecutablePipelineStage::SwitchableCompiledExecutablePipelineStage(
    std::unique_ptr<ExecutablePipelineStage> primary,
    std::unique_ptr<ExecutablePipelineStage> alternate,
    std::shared_ptr<std::atomic<int64_t>> selector,
    int64_t alternateExpectedValue)
    : primary(std::move(primary))
    , alternate(std::move(alternate))
    , selector(std::move(selector))
    , alternateExpectedValue(alternateExpectedValue)
{
    PRECONDITION(this->primary != nullptr, "SwitchableCompiledExecutablePipelineStage requires a non-null primary stage");
    PRECONDITION(this->alternate != nullptr, "SwitchableCompiledExecutablePipelineStage requires a non-null alternate stage");
    PRECONDITION(this->selector != nullptr, "SwitchableCompiledExecutablePipelineStage requires a non-null selector atomic");
}

void SwitchableCompiledExecutablePipelineStage::start(PipelineExecutionContext& pipelineExecutionContext)
{
    primary->start(pipelineExecutionContext);
    alternate->start(pipelineExecutionContext);
}

void SwitchableCompiledExecutablePipelineStage::execute(
    const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    auto* selected = (selector->load(std::memory_order_relaxed) == alternateExpectedValue) ? alternate.get() : primary.get();
    selected->execute(inputTupleBuffer, pipelineExecutionContext);
}

void SwitchableCompiledExecutablePipelineStage::stop(PipelineExecutionContext& pipelineExecutionContext)
{
    primary->stop(pipelineExecutionContext);
    alternate->stop(pipelineExecutionContext);
}

std::ostream& SwitchableCompiledExecutablePipelineStage::toString(std::ostream& os) const
{
    os << "SwitchableCompiledExecutablePipelineStage(primary=" << *primary << ", alternate=" << *alternate
       << ", alternateExpectedValue=" << alternateExpectedValue << ")";
    return os;
}

}
