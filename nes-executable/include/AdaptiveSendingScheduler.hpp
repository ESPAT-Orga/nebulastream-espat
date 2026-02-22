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
#include <map>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <BackpressureStatisticsListener.hpp>

namespace NES
{

using Priority = uint64_t;
struct ChannelData
{
    std::string channelId;
    Priority priority;
};

struct AdaptiveSendingScheduler : BackpressureStatisticListener {
    void onEvent(BackpressureEvent event) override;
    void applyPressure(const std::string& channelId);
    void releasePressure(const std::string& channelId);
    bool canSend(const std::string& channelId);
    void addChannel(const std::string& channelId, Priority priority);
private:
    std::unordered_map<std::string, ChannelData> channels;
    std::map<Priority, std::vector<std::string>> underBackpressure;
    //TODO: remove this once we record more priorities
    Priority maxPriority = 0;
};

}
