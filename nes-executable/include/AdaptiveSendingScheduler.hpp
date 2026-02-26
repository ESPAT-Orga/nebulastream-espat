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

namespace NES
{

using Priority = uint64_t;

constexpr Priority INVALID_PRIORITY = 0;

struct RegisteredChannel
{
    std::string channelId;
    Priority priority;
    std::reference_wrapper<std::atomic<bool>> blockedFlag;
};

using LockedPriorityMap = folly::LockedPtr<std::map<Priority, std::vector<std::string>>, std::mutex>;

struct AdaptiveSendingScheduler : BackpressureStatisticListener {
    void onEvent(BackpressureEvent event) override;
    void applyPressure(const std::string& channelId);
    void unbufferingCompleted(const std::string& channelId);
    bool canSend(const std::string& channelId);
    void registerChannel(const std::string& channelId, std::atomic<bool>& blockedFlag);
    void setBlockedStatusForPriorityRange(Priority start, Priority end, bool blocked, LockedPriorityMap lockedPriorities);

private:
    //TODO: this one we can probably remove soon
    folly::Synchronized<std::unordered_map<std::string, RegisteredChannel>> registeredChannels;
    folly::Synchronized<std::map<std::string, std::reference_wrapper<RegisteredChannel>>> priorities;

    folly::Synchronized<std::map<Priority, std::vector<std::string>>> underBackpressure;
    std::atomic<Priority> minPriorityUnderPressure = INVALID_PRIORITY;
    //TODO: remove this once we record more priorities
    std::atomic<Priority> maxPriority = 1;
};

}
