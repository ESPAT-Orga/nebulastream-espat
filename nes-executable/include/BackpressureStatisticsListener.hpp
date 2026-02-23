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
    BaseBackpressureEvent(const std::string& channelId) : channelId(channelId) { }

    BaseBackpressureEvent() = default;

    ChronoClock::time_point timestamp = ChronoClock::now();
    //record the channel id so that backpressure events can be matched to a specific query
    std::string channelId = "INVALID";
};

struct ApplyPressureEvent : BaseBackpressureEvent
{
    ApplyPressureEvent(const std::string& channelId) : BaseBackpressureEvent(channelId) { }

    ApplyPressureEvent() = default;
};

struct ReleasePressureEvent : BaseBackpressureEvent
{
    ReleasePressureEvent(const std::string& channelId) : BaseBackpressureEvent(channelId) { }

    ReleasePressureEvent() = default;
};

struct UnbufferingCompletedEvent : BaseBackpressureEvent
{
    UnbufferingCompletedEvent(const std::string& channelId) : BaseBackpressureEvent(channelId) { }

    UnbufferingCompletedEvent() = default;
};

using BackpressureEvent = std::variant<ApplyPressureEvent, ReleasePressureEvent, UnbufferingCompletedEvent>;

struct BackpressureStatisticListener
{
    virtual ~BackpressureStatisticListener() = default;

    /// This function is called from a WorkerThread!
    /// It should not block, and it has to be thread-safe!
    virtual void onEvent(BackpressureEvent event) = 0;
};

}
