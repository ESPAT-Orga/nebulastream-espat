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
#include <folly/Synchronized.h>
#include <BackpressureStatisticsListener.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

using Priority = uint64_t;

constexpr Priority INVALID_PRIORITY = 0;

struct RegisteredChannel
{
    LocalQueryId localQueryId;
    Priority priority;
    std::reference_wrapper<std::atomic<bool>> blockedFlag;
};

// using LockedPriorityMap = folly::LockedPtr<std::map<Priority, std::vector<std::string>>, std::mutex>;
// using LockedPriorityMap = folly::LockedPtr<std::mutex, std::map<Priority, std::vector<std::string>>>;
// using LockedPriorityMap = folly::LockedPtr<std::map<Priority, std::vector<std::string>>, folly::detail::SynchronizedLockPolicyShared>;

struct AdaptiveSendingScheduler : BackpressureStatisticListener {
    void onEvent(BackpressureEvent event) override;
    void applyPressure(LocalQueryId localQueryId, Priority priority);
    void unbufferingCompleted(LocalQueryId localQueryId, Priority priority);
    void registerChannel(LocalQueryId localQueryId, Priority priority, std::atomic<bool>& blockedFlag);

    template<typename LockedPriorityMap>
    void setBlockedStatusForPriorityRange(Priority start, Priority end, bool blocked, LockedPriorityMap lockedPriorities)
    {
        if (end == INVALID_PRIORITY)
        {
            end = lockedPriorities->end()->first;
        }
        auto begin = lockedPriorities->upper_bound(start);
        auto endIt = lockedPriorities->lower_bound(end);
        INVARIANT(begin != lockedPriorities->end(), "Start priority not found");
        INVARIANT(endIt != lockedPriorities->end(), "End priority not found");

        for (auto& [_, ch] : std::ranges::subrange(begin, endIt))
            // for (auto it = begin; it != endIt; ++it)
        {
            // it->second.blockedFlag.get().store(blocked);
            NES_DEBUG("Setting blocked status for channel id = {} to {}", ch.localQueryId, blocked);
            ch.blockedFlag.get().store(blocked);
        }
    }

private:
    //TODO: this one we can probably remove soon
    // folly::Synchronized<std::unordered_map<LocalQueryId, RegisteredChannel>> registeredChannels;
    // folly::Synchronized<std::map<Priority, std::reference_wrapper<RegisteredChannel>>> priorities;
    folly::Synchronized<std::map<Priority, RegisteredChannel>> priorities;

    folly::Synchronized<std::map<Priority, std::vector<LocalQueryId>>> underBackpressure;
    std::atomic<Priority> minPriorityUnderPressure = INVALID_PRIORITY;
    //TODO: remove this once we record more priorities
    std::atomic<Priority> maxPriority = 1;
};

}
