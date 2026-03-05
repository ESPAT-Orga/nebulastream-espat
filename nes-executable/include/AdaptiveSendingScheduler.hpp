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
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <variant>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <folly/MPMCQueue.h>
#include <folly/Synchronized.h>
#include <BackpressureStatisticsListener.hpp>
#include <ErrorHandling.hpp>

#include "Thread.hpp"

namespace NES
{

using LowPriority = bool;

// constexpr Priority INVALID_PRIORITY = 0;

struct RegisteredChannel
{
    LocalQueryId localQueryId;
    std::reference_wrapper<std::atomic<uint64_t>> contingent;
};

struct AdaptiveSendingScheduler : TrafficStatisticListener {
    void onEvent(BackpressureEvent event) override;
    void threadRoutine(const std::stop_token& token);
    void assignContingents();
    void applyPressure(LocalQueryId localQueryId);
    void unbufferingCompleted(LocalQueryId localQueryId);
    void start();
    LowPriority registerChannel(LocalQueryId localQueryId, LowPriority throttled, std::atomic<uint64_t>& contingent);
    void unregisterChannel(LocalQueryId localQueryId);

    template<typename LockedPriorityMap>
    void setBlockedStatusForPriorityRange(LowPriority start, LowPriority end, bool blocked, LockedPriorityMap lockedPriorities)
    {
        NES_DEBUG("Setting blocked status for priority range {} - {} to {}", start, end, blocked);
        if (start >= end)
        {
            NES_DEBUG("No blocking to set for range: {} - {}", start, end);
            return;
        }
        auto begin = lockedPriorities->upper_bound(start);
        auto endIt = lockedPriorities->upper_bound(end);

        for (auto& [_, ch] : std::ranges::subrange(begin, endIt))
        {
            NES_DEBUG("Setting blocked status for channel id = {} to {}", ch.localQueryId, blocked);
            ch.blockedFlag.get().store(blocked);
        }
    }

private:
    folly::Synchronized<std::map<LocalQueryId, RegisteredChannel>> high_priority;
    folly::Synchronized<std::map<LocalQueryId, RegisteredChannel>> low_priority;

    std::map<LocalQueryId, RegisteredChannel&> high_priority_under_backpressure;

    static constexpr size_t QUEUE_LENGTH = 1000;
    folly::MPMCQueue<BackpressureEvent> events{QUEUE_LENGTH};
    Thread schedulerThread;
    std::atomic<bool> init_throttled{false};
};

}
