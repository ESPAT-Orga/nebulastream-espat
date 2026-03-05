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

#include <ranges>
#include <Util/Overloaded.hpp>
#include <folly/Synchronized.h>

#include <ErrorHandling.hpp>

#include "Thread.hpp"

namespace NES
{
constexpr uint64_t READ_RETRY_MS = 100;
constexpr uint64_t DROP_LOG_INTERVAL = 100;
constexpr uint64_t ASSIGN_INTERVAL_MS = 1000;
constexpr uint64_t CONTINGET_PER_INTERVAL = 1;

// namespace
// {
// void warnOnOverflow(bool writeFailed)
// {
//     if (writeFailed) [[unlikely]]
//     {
//         static std::atomic<uint64_t> droppedCount{0};
//         /// Log first drop immediately, then every DROP_LOG_INTERVAL
//         if (uint64_t dropped = droppedCount.fetch_add(1, std::memory_order_relaxed) + 1; dropped == 1 || dropped % DROP_LOG_INTERVAL == 0)
//         {
//             NES_WARNING("Event queue full, {} events dropped so far", dropped);
//         }
//     }
// }
// }

void AdaptiveSendingScheduler::onEvent(BackpressureEvent event)
{
    (void)event;
    // warnOnOverflow(
    //     !events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return BackpressureEvent(std::forward<T>(arg)); }, std::move(event))));
}

void AdaptiveSendingScheduler::threadRoutine(const std::stop_token& token)
{
    while (!token.stop_requested())
    {
        assignContingents();
        std::this_thread::sleep_for(std::chrono::milliseconds(ASSIGN_INTERVAL_MS));
        BackpressureEvent event = ReleasePressureEvent{}; /// Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(READ_RETRY_MS), event))
        {
            continue;
        }
        std::visit(
            Overloaded{
                [&](const UnbufferingCompletedEvent& unbufferEvent)
                {
                    unbufferingCompleted(unbufferEvent.localQueryId);
                },
                [&](const ApplyPressureEvent& applyEvent)
                {
                    applyPressure(applyEvent.localQueryId);
                },
                [&](const BufferSendEvent&)
                {
                },
                [&](const BufferIngestEvent&)
                {
                },
                [&](const AdaptivelyBlockSendingEvent&)
                {
                },
                [&](const ReleasePressureEvent&)
                {
                    //no need to do anything, as we still wait for all buffers to be unbuffered
                }},
            event);
    }
}

void AdaptiveSendingScheduler::assignContingents()
{
    auto locked = low_priority.wlock();
    for (auto ch : std::views::values(*locked))
    {
        NES_DEBUG("Setting contingent for query {} to {}", ch.localQueryId, CONTINGET_PER_INTERVAL);
        ch.contingent.get().store(CONTINGET_PER_INTERVAL);
    }
}

void AdaptiveSendingScheduler::applyPressure(const LocalQueryId localQueryId)
{
    NES_DEBUG("Applying pressure to channel id = {}", localQueryId);
    auto locked = high_priority.wlock();
    auto it = locked->find(localQueryId);
    if (it != locked->end())
    {
        NES_DEBUG("Channel {} does not have high priority, nothing to record", localQueryId);
        return;
    }

    high_priority_under_backpressure.insert({localQueryId, it->second});
}

void AdaptiveSendingScheduler::unbufferingCompleted(const LocalQueryId localQueryId)
{
    NES_DEBUG("Unbuffering completed channel id = {}", localQueryId);

    auto locked = high_priority.wlock();
    auto it = locked->find(localQueryId);
    if (it != locked->end())
    {
        NES_DEBUG("Channel {} does not have high priority, nothing to record", localQueryId);
        return;
    }

    INVARIANT(high_priority_under_backpressure.contains(localQueryId), "Channel {} is not under backpressure", localQueryId);
    high_priority_under_backpressure.erase(localQueryId);
}

void AdaptiveSendingScheduler::start()
{
    schedulerThread = Thread("adaptive-sending-scheduler", [this](const std::stop_token& stopToken) { threadRoutine(stopToken); });
}

//TODO: we only return priority because propagating it does not work yet
LowPriority AdaptiveSendingScheduler::registerChannel(const LocalQueryId localQueryId, LowPriority throttled, std::atomic<uint64_t>& contingent)
{
    NES_DEBUG("Registered channel id = {}, priority = {}", localQueryId, throttled);
    auto registeredChannel = RegisteredChannel{.localQueryId = localQueryId, .contingent = std::ref(contingent)};
    //TODO remove
    auto expected = false;
    throttled = not init_throttled.compare_exchange_strong(expected, true);
    if (throttled)
    {
        auto locked = low_priority.wlock();
        if (not locked->contains(localQueryId))
            locked->insert({localQueryId, registeredChannel});
    } else
    {
        auto locked = high_priority.wlock();
        if (not locked->contains(localQueryId))
            locked->insert({localQueryId, registeredChannel});
    }
    return throttled;
}

void AdaptiveSendingScheduler::unregisterChannel(LocalQueryId localQueryId)
{
    auto locked = high_priority.wlock();
    if (locked->erase(localQueryId))
    {
        return;
    }

    locked = low_priority.wlock();
    INVARIANT(locked->contains(localQueryId), "Channel for local query {} was not found", localQueryId);
    locked->erase(localQueryId);
}


}