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

namespace NES
{
using ChronoClock = std::chrono::system_clock;

struct BaseBufferManagerEvent
{
    // BaseBufferManagerEvent(WorkerThreadId threadId, QueryId queryId) : threadId(threadId), queryId(queryId) { }

    BaseBufferManagerEvent() = default;
    BaseBufferManagerEvent(std::optional<std::variant<PipelineId, OriginId>> pipelineId) : pipelineId(pipelineId) {}

    ChronoClock::time_point timestamp = ChronoClock::now();
    std::optional<std::variant<PipelineId, OriginId>> pipelineId = std::nullopt;
    // WorkerThreadId threadId = INVALID<WorkerThreadId>;
    // QueryId queryId = INVALID<QueryId>;
};

struct GetBufferEvent : BaseBufferManagerEvent
{
    // GetBufferEvent(WorkerThreadId threadId, QueryId queryId, size_t bufferSize)
    //     : BaseBufferManagerEvent(threadId, queryId), bufferSize(bufferSize)
    explicit GetBufferEvent(size_t bufferSize, std::optional<std::variant<PipelineId, OriginId>> pipelineId) : BaseBufferManagerEvent(pipelineId), bufferSize(bufferSize){ }

    GetBufferEvent() = default;

    size_t bufferSize{};
};

struct GetUnpooledBufferEvent : BaseBufferManagerEvent
{
    // GetBufferEvent(WorkerThreadId threadId, QueryId queryId, size_t bufferSize)
    //     : BaseBufferManagerEvent(threadId, queryId), bufferSize(bufferSize)
    explicit GetUnpooledBufferEvent(size_t bufferSize, std::optional<std::variant<PipelineId, OriginId>> pipelineId) : BaseBufferManagerEvent(pipelineId), bufferSize(bufferSize) { }

    GetUnpooledBufferEvent() = default;

    size_t bufferSize{};
};

struct RecyclePooledBufferEvent : BaseBufferManagerEvent
{
    // GetBufferEvent(WorkerThreadId threadId, QueryId queryId, size_t bufferSize)
    //     : BaseBufferManagerEvent(threadId, queryId), bufferSize(bufferSize)
    explicit RecyclePooledBufferEvent(size_t bufferSize, std::optional<std::variant<PipelineId, OriginId>> pipelineId) : BaseBufferManagerEvent(pipelineId), bufferSize(bufferSize) { }

    RecyclePooledBufferEvent() = default;

    size_t bufferSize{};
};

struct RecycleUnpooledBufferEvent : BaseBufferManagerEvent
{
    // GetBufferEvent(WorkerThreadId threadId, QueryId queryId, size_t bufferSize)
    //     : BaseBufferManagerEvent(threadId, queryId), bufferSize(bufferSize)
    explicit RecycleUnpooledBufferEvent(size_t bufferSize, std::optional<std::variant<PipelineId, OriginId>> pipelineId) : BaseBufferManagerEvent(pipelineId), bufferSize(bufferSize) { }

    RecycleUnpooledBufferEvent() = default;

    size_t bufferSize{};
};

using BufferManagerEvent = std::variant<GetBufferEvent, RecyclePooledBufferEvent>;
static_assert(std::is_default_constructible_v<BufferManagerEvent>, "Events should be default constructible");

struct BufferManagerStatisticListener
{
    virtual ~BufferManagerStatisticListener() = default;

    /// This function is called from a WorkerThread!
    /// It should not block, and it has to be thread-safe!
    virtual void onEvent(BufferManagerEvent event) = 0;
};
}
