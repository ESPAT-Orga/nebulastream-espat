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

#include "Thread.hpp"

namespace NES {
constexpr uint64_t READ_RETRY_MS = 100;
constexpr uint64_t DROP_LOG_INTERVAL = 100;
namespace
{
void warnOnOverflow(bool writeFailed)
{
    if (writeFailed) [[unlikely]]
    {
        static std::atomic<uint64_t> droppedCount{0};
        /// Log first drop immediately, then every DROP_LOG_INTERVAL
        if (uint64_t dropped = droppedCount.fetch_add(1, std::memory_order_relaxed) + 1; dropped == 1 || dropped % DROP_LOG_INTERVAL == 0)
        {
            NES_WARNING("Event queue full, {} events dropped so far", dropped);
        }
    }
}
}

void AdaptiveSendingScheduler::onEvent(BackpressureEvent event)
{

    warnOnOverflow(
        !events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return BackpressureEvent(std::forward<T>(arg)); }, std::move(event))));
}


void AdaptiveSendingScheduler::threadRoutine(const std::stop_token& token)
{
    while (!token.stop_requested())
    {
        BackpressureEvent event = ReleasePressureEvent{}; /// Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(READ_RETRY_MS), event))
        {
            continue;
        }
        std::visit(
            Overloaded{
                [&](const UnbufferingCompletedEvent& unbufferEvent)
                {
                    unbufferingCompleted(unbufferEvent.localQueryId, unbufferEvent.priority);
                },
                [&](const ApplyPressureEvent& applyEvent)
                {
                    applyPressure(applyEvent.localQueryId, applyEvent.priority);
                },
                [&](const ReleasePressureEvent&)
                {
                    //no need to do anything, as we still wait for all buffers to be unbuffered
                }},
            event);
    }
}

void AdaptiveSendingScheduler::applyPressure(const LocalQueryId localQueryId, Priority priority)
{
    NES_DEBUG("Applying pressure to channel id = {} with priority {}", localQueryId, priority);
    auto lockedPriorities = priorities.wlock();
    Priority minPrioOld;
    Priority minPrioNew;
    {
        auto backpressureLocked = underBackpressure.wlock();
        backpressureLocked->operator[](priority).emplace_back(localQueryId);
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

void AdaptiveSendingScheduler::unbufferingCompleted(const LocalQueryId localQueryId, Priority priority)
{
    NES_DEBUG("Unbuffering completed channel id = {}, with priority {}", localQueryId, priority);

    Priority minPrioOld;
    Priority minPrioNew;
    {
        auto backpressureLocked = underBackpressure.wlock();
        auto& vec = backpressureLocked->operator[](priority);
        auto toRemove = std::ranges::find(vec, localQueryId);
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

void AdaptiveSendingScheduler::start()
{
    schedulerThread = Thread("adaptive-sending-scheduler", [this](const std::stop_token& stopToken) { threadRoutine(stopToken); });
}

//TODO: we only return priority because propagating it does not work yet
Priority AdaptiveSendingScheduler::registerChannel(const LocalQueryId localQueryId, Priority priority, std::atomic<bool>& blockedFlag)
{
    priority = maxPriority++;
    NES_DEBUG("Registered channel id = {}, priority = {}", localQueryId, priority);
    auto registeredChannel = RegisteredChannel {
    .localQueryId = localQueryId,
    .priority = priority,
    .blockedFlag = std::ref(blockedFlag)};
    priorities.wlock()->emplace(priority, registeredChannel);
    return priority;
}


}