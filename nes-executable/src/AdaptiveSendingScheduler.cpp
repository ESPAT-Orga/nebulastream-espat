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

#include <folly/Synchronized.h>

#include <ErrorHandling.hpp>

namespace NES {

bool AdaptiveSendingScheduler::canSend(const std::string& channelId) {
    if (!priorities.contains(channelId))
    {
        //TODO: remove once we got proper priorities
        priorities.emplace(channelId, maxPriority++);
    }
    return true;
}

void AdaptiveSendingScheduler::addChannel(const std::string& channelId, Priority priority)
{
    ChannelData channelData{channelId, priority};
    channels.emplace(channelId, channelData);
    
}

}