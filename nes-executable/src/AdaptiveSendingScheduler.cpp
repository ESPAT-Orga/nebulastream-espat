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

#include <AdaptiveSendingScheduler.hpp>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stop_token>
#include <utility>

#include <Util/Overloaded.hpp>
#include <folly/Synchronized.h>

#include <ErrorHandling.hpp>

namespace NES {

void AdaptiveSendingScheduler::onEvent(BackpressureEvent event)
{
        std::visit(
            Overloaded{
                [&](const UnbufferingCompletedEvent& unbufferEvent)
                {
                    unbufferingCompleted(unbufferEvent.channelId);
                },
                [&](const ApplyPressureEvent& applyEvent)
                {
                    applyPressure(applyEvent.channelId);
                },
                [&](const ReleasePressureEvent&)
                {
                    //no need to do anything, as we still wait for all buffers to be unbuffered
                }},
            event);
}

void AdaptiveSendingScheduler::applyPressure(const std::string& channelId)
{
    Priority priority;;
    {
        auto channelsLocked = channels.rlock();
        auto it = channelsLocked->find(channelId);
        //TODO do invariants get removed in release?
        INVARIANT(it != channelsLocked->end(), "Channel not found");
        priority = it->second;
    }

    auto backpressureLocked = underBackpressure.wlock();
    backpressureLocked->operator[](priority).emplace_back(channelId);
    minPriorityUnderPressure.store(backpressureLocked->begin()->first);
}

void AdaptiveSendingScheduler::unbufferingCompleted(const std::string& channelId)
{
    NES_DEBUG("Unbuffering completed channel id = {}", channelId);
    Priority priority;;
    {
        auto channelsLocked = channels.rlock();
        auto it = channelsLocked->find(channelId);
        //TODO do invariants get removed in release?
        INVARIANT(it != channelsLocked->end(), "Channel not found");
        priority = it->second;
    }

    auto backpressureLocked = underBackpressure.wlock();
    auto& vec = backpressureLocked->operator[](priority);
    auto toRemove = std::find(vec.begin(), vec.end(), channelId);
    INVARIANT(toRemove != vec.end(), "Channel not found in underBackpressure");
    vec.erase(toRemove);

    if (vec.empty())
    {
        backpressureLocked->erase(priority);
    }

    auto lowest = backpressureLocked->begin();
    if (lowest != backpressureLocked->end())
    {
        NES_DEBUG("New max priority under pressure: {}", lowest->first);
        minPriorityUnderPressure.store(lowest->first);
    } else
    {
        NES_DEBUG("No more under pressure channels");
        minPriorityUnderPressure.store(INVALID_PRIORITY);
    }
}


bool AdaptiveSendingScheduler::canSend(const std::string& channelId) {
    auto channelsLocked = channels.rlock();
    auto it = channelsLocked->find(channelId);
    Priority priority = 0;
    if (it == channelsLocked->end())
    {
        //TODO: remove once we got proper priorities
        channelsLocked.unlock();
        priority = maxPriority++;
        addChannel(channelId, priority);
    } else
    {
        INVARIANT(it != channelsLocked->end(), "Channel not found");
        priority = it->second;
    }
    channelsLocked.unlock();

    auto currMinPrio = minPriorityUnderPressure.load();
    NES_DEBUG("Can send: channelId={} currMinPrio={}, priority={}", channelId, currMinPrio, priority);
    return currMinPrio == INVALID_PRIORITY  || priority <= currMinPrio;
}

void AdaptiveSendingScheduler::addChannel(const std::string& channelId, Priority priority)
{
    channels.wlock()->emplace(channelId, priority);
}

}