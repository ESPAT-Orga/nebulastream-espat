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

#include <Runtime/AbstractBufferProvider.hpp>

namespace NES
{
using ChronoClock = std::chrono::system_clock;

struct BaseBufferManagerEvent
{
    BaseBufferManagerEvent() = default;

    BaseBufferManagerEvent(PipelineId pipelineID, size_t bufferSize) : pipelineId(pipelineID), bufferSize(bufferSize) { }

    ChronoClock::time_point timestamp = ChronoClock::now();
    PipelineId pipelineId = INVALID_PIPELINE_ID;
    size_t bufferSize{};
};

struct GetPooledBufferEvent : BaseBufferManagerEvent
{
    explicit GetPooledBufferEvent(size_t bufferSize, PipelineId pipelineID) : BaseBufferManagerEvent(pipelineID, bufferSize) { }

    GetPooledBufferEvent() = default;
};

struct GetUnpooledBufferEvent : BaseBufferManagerEvent
{
    explicit GetUnpooledBufferEvent(size_t bufferSize, PipelineId pipelineId) : BaseBufferManagerEvent(pipelineId, bufferSize) { }

    GetUnpooledBufferEvent() = default;
};

struct ReturnPooledBufferEvent : BaseBufferManagerEvent
{
    explicit ReturnPooledBufferEvent(size_t bufferSize, PipelineId pipelineId) : BaseBufferManagerEvent(pipelineId, bufferSize) { }

    ReturnPooledBufferEvent() = default;
};

struct RecycleUnpooledBufferEvent : BaseBufferManagerEvent
{
    explicit RecycleUnpooledBufferEvent(size_t bufferSize, PipelineId pipelineId) : BaseBufferManagerEvent(pipelineId, bufferSize) { }

    RecycleUnpooledBufferEvent() = default;
};

using BufferManagerEvent = std::variant<GetPooledBufferEvent, ReturnPooledBufferEvent, GetUnpooledBufferEvent, RecycleUnpooledBufferEvent>;
static_assert(std::is_default_constructible_v<BufferManagerEvent>, "Events should be default constructible");

struct BufferManagerStatisticListener
{
    virtual ~BufferManagerStatisticListener() = default;

    /// This function is called from a WorkerThread!
    /// It should not block, and it has to be thread-safe!
    virtual void onEvent(BufferManagerEvent event) = 0;
};
}
