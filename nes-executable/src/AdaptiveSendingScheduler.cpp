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
                [&](const ApplyPressureEvent& applyEvent)
                {
                    applyPressure(applyEvent.channelId);
                },
                [&](const ReleasePressureEvent& releaseEvent)
                {
                    releasePressure(releaseEvent.channelId);
                }},
            event);
}

void AdaptiveSendingScheduler::applyPressure(const std::string& channelId)
{
    auto channel = channels.find(channelId);
    INVARIANT(channel != channels.end(), "Channel not found");

    underBackpressure[channel->second.priority].emplace_back(channel->second.channelId);
}

void AdaptiveSendingScheduler::releasePressure(const std::string& channelId)
{
    auto channel = channels.find(channelId);
    INVARIANT(channel != channels.end(), "Channel not found");

    auto& vec = underBackpressure[channel->second.priority];
    auto toRemove = std::find(vec.begin(), vec.end(), channelId);
    INVARIANT(toRemove != vec.end(), "Channel not found in underBackpressure");
    vec.erase(toRemove);

    if (vec.empty())
    {
        underBackpressure.erase(channel->second.priority);
    }

}


bool AdaptiveSendingScheduler::canSend(const std::string& channelId) {
    if (!channels.contains(channelId))
    {
        //TODO: remove once we got proper priorities
        addChannel(channelId, maxPriority++);
    }

    auto channel = channels.find(channelId);
    INVARIANT(channel != channels.end(), "Channel not found");

    // check if any channel with lower priority is experiencing backpressure
    auto it = underBackpressure.lower_bound(channel->second.priority);
    return it != underBackpressure.begin();
}

void AdaptiveSendingScheduler::addChannel(const std::string& channelId, Priority priority)
{
    ChannelData channelData{channelId, priority};
    channels.emplace(channelId, channelData);
}

}