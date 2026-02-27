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

struct BaseTrafficEvent
{
    BaseTrafficEvent(const LocalQueryId localQueryId, uint64_t priority) : localQueryId(localQueryId), priority(priority) { }

    BaseTrafficEvent() = default;

    ChronoClock::time_point timestamp = ChronoClock::now();
    //record the channel id so that backpressure events can be matched to a specific query
    LocalQueryId localQueryId = INVALID_LOCAL_QUERY_ID;
    uint64_t priority = 0;
};

struct ApplyPressureEvent : BaseTrafficEvent
{
    ApplyPressureEvent(const LocalQueryId localQueryId, uint64_t priority) : BaseTrafficEvent(localQueryId, priority) { }

    ApplyPressureEvent() = default;
};

struct ReleasePressureEvent : BaseTrafficEvent
{
    ReleasePressureEvent(const LocalQueryId localQueryId, uint64_t priority) : BaseTrafficEvent(localQueryId, priority) { }

    ReleasePressureEvent() = default;
};

struct UnbufferingCompletedEvent : BaseTrafficEvent
{
    UnbufferingCompletedEvent(const LocalQueryId localQueryId, uint64_t priority) : BaseTrafficEvent(localQueryId, priority) { }

    UnbufferingCompletedEvent() = default;
};

struct BufferSendEvent : BaseTrafficEvent
{
    BufferSendEvent(const LocalQueryId localQueryId, uint64_t priority, uint64_t numberOfTuples) : BaseTrafficEvent(localQueryId, priority), numberOfTuples(numberOfTuples) { }

    BufferSendEvent() = default;
    uint64_t numberOfTuples;
};

struct BufferIngestEvent : BaseTrafficEvent
{
    BufferIngestEvent(const LocalQueryId localQueryId, uint64_t numberOfTuples) : BaseTrafficEvent(localQueryId, 0), numberOfTuples(numberOfTuples) { }

    BufferIngestEvent() = default;
    uint64_t numberOfTuples = 0;
};

struct AdaptivelyBlockSendingEvent : BaseTrafficEvent
{
    AdaptivelyBlockSendingEvent(const LocalQueryId localQueryId, uint64_t priority) : BaseTrafficEvent(localQueryId, priority) { }

    AdaptivelyBlockSendingEvent() = default;
};

using BackpressureEvent = std::variant<ApplyPressureEvent, ReleasePressureEvent, UnbufferingCompletedEvent, BufferSendEvent, BufferIngestEvent, AdaptivelyBlockSendingEvent>;

struct TrafficStatisticListener
{
    virtual ~TrafficStatisticListener() = default;

    /// This function is called from a WorkerThread!
    /// It should not block, and it has to be thread-safe!
    virtual void onEvent(BackpressureEvent event) = 0;
};

}
