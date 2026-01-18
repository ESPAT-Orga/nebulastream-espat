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

struct BaseBackpressureEvent
{
    BaseBackpressureEvent(WorkerThreadId threadId, LocalQueryId queryId) : threadId(threadId), queryId(queryId) { }

    BaseBackpressureEvent() = default;

    ChronoClock::time_point timestamp = ChronoClock::now();
    WorkerThreadId threadId = INVALID<WorkerThreadId>;
    LocalQueryId queryId = INVALID_LOCAL_QUERY_ID;
};

struct ApplyPressureEvent : BaseBackpressureEvent
{
    ApplyPressureEvent(WorkerThreadId threadId, LocalQueryId queryId)
        : BaseBackpressureEvent(threadId, queryId)
    {
    }

    ApplyPressureEvent() = default;
};

struct ReleasePressureEvent : BaseBackpressureEvent
{
    ReleasePressureEvent(WorkerThreadId threadId, LocalQueryId queryId)
        : BaseBackpressureEvent(threadId, queryId)
    {
    }

    ReleasePressureEvent() = default;
};

using BackpressureEvent = std::variant<ApplyPressureEvent, ReleasePressureEvent>;

struct BackpressureStatisticListener
{
    virtual ~BackpressureStatisticListener() = default;

    /// This function is called from a WorkerThread!
    /// It should not block, and it has to be thread-safe!
    virtual void onEvent(BackpressureEvent event) = 0;
};

}
