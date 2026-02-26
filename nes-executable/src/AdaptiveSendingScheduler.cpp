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

#include <algorithm>
#include <atomic>
#include <functional>

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
    auto regLocked = registeredChannels.rlock();
    auto it = regLocked->find(channelId);
    INVARIANT(it != regLocked->end(), "Channel not found");
    const Priority priority = it->second.priority;

    {
        auto backpressureLocked = underBackpressure.wlock();
        backpressureLocked->operator[](priority).emplace_back(channelId);
        minPriorityUnderPressure.store(backpressureLocked->begin()->first);
    }

    const Priority minPrio = minPriorityUnderPressure.load();
    for (auto& [id, ch] : *regLocked)
    {
        ch.blockedFlag.get().store(ch.priority > minPrio);
    }
}

void AdaptiveSendingScheduler::unbufferingCompleted(const std::string& channelId)
{
    NES_DEBUG("Unbuffering completed channel id = {}", channelId);

    auto regLocked = registeredChannels.rlock();
    auto it = regLocked->find(channelId);
    INVARIANT(it != regLocked->end(), "Channel not found");
    const Priority priority = it->second.priority;

    {
        auto backpressureLocked = underBackpressure.wlock();
        auto& vec = backpressureLocked->operator[](priority);
        auto toRemove = std::ranges::find(vec, channelId);
        INVARIANT(toRemove != vec.end(), "Channel not found in underBackpressure");
        vec.erase(toRemove);

        if (vec.empty())
        {
            backpressureLocked->erase(priority);
        }

        auto lowest = backpressureLocked->begin();
        if (lowest != backpressureLocked->end())
        {
            NES_DEBUG("New min priority under pressure: {}", lowest->first);
            minPriorityUnderPressure.store(lowest->first);
        }
        else
        {
            NES_DEBUG("No more under pressure channels");
            minPriorityUnderPressure.store(INVALID_PRIORITY);
        }
    }

    const Priority minPrio = minPriorityUnderPressure.load();
    for (auto& [id, ch] : *regLocked)
    {
        ch.blockedFlag.get().store(minPrio != INVALID_PRIORITY && ch.priority > minPrio);
    }
}

bool AdaptiveSendingScheduler::canSend(const std::string& channelId)
{
    auto regLocked = registeredChannels.rlock();
    auto it = regLocked->find(channelId);
    INVARIANT(it != regLocked->end(), "Channel not found");
    const Priority priority = it->second.priority;
    regLocked.unlock();

    const Priority currMinPrio = minPriorityUnderPressure.load();
    NES_DEBUG("Can send: channelId={} currMinPrio={}, priority={}", channelId, currMinPrio, priority);
    return currMinPrio == INVALID_PRIORITY || priority <= currMinPrio;
}

void AdaptiveSendingScheduler::registerChannel(const std::string& channelId, std::atomic<bool>& blockedFlag)
{
    const Priority priority = maxPriority++;
    registeredChannels.wlock()->emplace(channelId, RegisteredChannel{.priority = priority, .blockedFlag = std::ref(blockedFlag)});
}

void AdaptiveSendingScheduler::setBlockedStatusForPriorityRange(Priority start, Priority end, bool blocked, LockedPriorityMap lockedPriorities)
{


}

}