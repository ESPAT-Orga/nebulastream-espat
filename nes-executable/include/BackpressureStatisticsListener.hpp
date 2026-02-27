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
#include <chrono>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>

namespace NES
{
using ChronoClock = std::chrono::system_clock;

//TODO: should we add an event to record when a query was throttled?
struct BaseBackpressureEvent
{
    BaseBackpressureEvent(const LocalQueryId localQueryId, uint64_t priority) : localQueryId(localQueryId), priority(priority) { }

    BaseBackpressureEvent() = default;

    ChronoClock::time_point timestamp = ChronoClock::now();
    //record the channel id so that backpressure events can be matched to a specific query
    LocalQueryId localQueryId = INVALID_LOCAL_QUERY_ID;
    //TODO: use proper type
    uint64_t priority = 0;
};

struct ApplyPressureEvent : BaseBackpressureEvent
{
    ApplyPressureEvent(const LocalQueryId localQueryId, uint64_t priority) : BaseBackpressureEvent(localQueryId, priority) { }

    ApplyPressureEvent() = default;
};

struct ReleasePressureEvent : BaseBackpressureEvent
{
    ReleasePressureEvent(const LocalQueryId localQueryId, uint64_t priority) : BaseBackpressureEvent(localQueryId, priority) { }

    ReleasePressureEvent() = default;
};

struct UnbufferingCompletedEvent : BaseBackpressureEvent
{
    UnbufferingCompletedEvent(const LocalQueryId localQueryId, uint64_t priority) : BaseBackpressureEvent(localQueryId, priority) { }

    UnbufferingCompletedEvent() = default;
};

//TODO: if we keep this in, then we need to rename the class
struct BufferSendEvent : BaseBackpressureEvent
{
    BufferSendEvent(const LocalQueryId localQueryId, uint64_t priority) : BaseBackpressureEvent(localQueryId, priority) { }

    BufferSendEvent() = default;
};

struct BufferIngestEvent : BaseBackpressureEvent
{
    BufferIngestEvent(const LocalQueryId localQueryId) : BaseBackpressureEvent(localQueryId, 0) { }

    BufferIngestEvent() = default;
};

using BackpressureEvent = std::variant<ApplyPressureEvent, ReleasePressureEvent, UnbufferingCompletedEvent, BufferSendEvent, BufferIngestEvent>;

struct BackpressureStatisticListener
{
    virtual ~BackpressureStatisticListener() = default;

    /// This function is called from a WorkerThread!
    /// It should not block, and it has to be thread-safe!
    virtual void onEvent(BackpressureEvent event) = 0;
};

}
