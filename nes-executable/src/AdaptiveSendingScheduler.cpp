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
#include <ranges>

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
    //TODO: replace this with param
    auto regLocked = registeredChannels.rlock();
    auto it = regLocked->find(channelId);
    INVARIANT(it != regLocked->end(), "Channel not found");
    const Priority priority = it->second.priority;
    regLocked.unlock();

    auto lockedPriorities = priorities.wlock();
    Priority minPrioOld;
    Priority minPrioNew;
    {
        auto backpressureLocked = underBackpressure.wlock();
        backpressureLocked->operator[](priority).emplace_back(channelId);
        minPrioNew = backpressureLocked->begin()->first;
        minPrioOld = minPriorityUnderPressure.exchange(minPrioNew);
    }

    setBlockedStatusForPriorityRange(minPrioNew, minPrioOld, true, std::move(lockedPriorities));
    // auto begin = lockedPriorities->upper_bound(minPrioNew);
    // auto endIt = lockedPriorities->lower_bound(minPrioOld);
    // INVARIANT(begin != lockedPriorities->end(), "Start priority not found");
    // INVARIANT(endIt != lockedPriorities->end(), "End priority not found");
    //
    // for (auto& [_, ch] : std::ranges::subrange(begin, endIt))
    //     // for (auto it = begin; it != endIt; ++it)
    // {
    //     // it->second.blockedFlag.get().store(blocked);
    //     ch.get().blockedFlag.get().store(true);
    // }
}

void AdaptiveSendingScheduler::unbufferingCompleted(const std::string& channelId)
{
    NES_DEBUG("Unbuffering completed channel id = {}", channelId);

    auto regLocked = registeredChannels.rlock();
    auto it = regLocked->find(channelId);
    INVARIANT(it != regLocked->end(), "Channel not found");
    const Priority priority = it->second.priority;

    Priority minPrioOld;
    Priority minPrioNew;
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
            minPrioNew = lowest->first;
            NES_DEBUG("New min priority under pressure: {}", minPrioNew);
        }
        else
        {
            minPrioNew = INVALID_PRIORITY;
            NES_DEBUG("No more under pressure channels");
        }
        minPrioOld = minPriorityUnderPressure.exchange(minPrioNew);
        auto lockedPriorities = priorities.wlock();

        setBlockedStatusForPriorityRange(minPrioOld, minPrioNew, false, std::move(lockedPriorities));
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

void AdaptiveSendingScheduler::registerChannel(const std::string& channelId, Priority priority, std::atomic<bool>& blockedFlag)
{
    //TODO remove
    priority = maxPriority++;
    registeredChannels.wlock()->emplace(channelId, RegisteredChannel{.channelId = channelId, .priority = priority, .blockedFlag = std::ref(blockedFlag)});
}


}