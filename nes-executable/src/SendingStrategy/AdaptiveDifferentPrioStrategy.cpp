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

#include <SendingStrategy/AdaptiveDifferentPrioStrategy.hpp>

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <Util/Logger/Logger.hpp>
#include <Priority.hpp>
#include <QueryId.hpp>

namespace NES
{

void AdaptiveDifferentPrioStrategy::registerChannel(QueryId queryId, Priority priority)
{
    const std::unique_lock lock(mutex);
    if (auto [it, inserted] = channels.try_emplace(queryId, ChannelState{priority, false}); !inserted)
    {
        NES_DEBUG("AdaptiveDifferentPrioStrategy: channel for {} already registered, updating priority", queryId);
        it->second.priority = priority;
    }
}

void AdaptiveDifferentPrioStrategy::deregisterChannel(QueryId queryId)
{
    const std::unique_lock lock(mutex);
    const auto it = channels.find(queryId);
    if (it == channels.end())
    {
        return;
    }
    if (it->second.priority == Priority::HIGH && it->second.inBackpressure)
    {
        highChannelsUnderBackpressure.fetch_sub(1, std::memory_order_relaxed);
    }
    channels.erase(it);
}

bool AdaptiveDifferentPrioStrategy::maySend(QueryId queryId) const
{
    const std::shared_lock lock(mutex);
    const auto it = channels.find(queryId);
    if (it == channels.end())
    {
        /// Unregistered channels default to allowed, matching ALWAYS_SEND.
        return true;
    }
    if (it->second.priority == Priority::HIGH)
    {
        return true;
    }
    return highChannelsUnderBackpressure.load(std::memory_order_relaxed) == 0;
}

void AdaptiveDifferentPrioStrategy::onBackpressureApplied(QueryId queryId)
{
    const std::unique_lock lock(mutex);
    const auto it = channels.find(queryId);
    if (it == channels.end() || it->second.priority != Priority::HIGH || it->second.inBackpressure)
    {
        return;
    }
    it->second.inBackpressure = true;
    highChannelsUnderBackpressure.fetch_add(1, std::memory_order_relaxed);
}

void AdaptiveDifferentPrioStrategy::onBackpressureReleased(QueryId queryId)
{
    const std::unique_lock lock(mutex);
    const auto it = channels.find(queryId);
    if (it == channels.end() || it->second.priority != Priority::HIGH || !it->second.inBackpressure)
    {
        return;
    }
    it->second.inBackpressure = false;
    highChannelsUnderBackpressure.fetch_sub(1, std::memory_order_relaxed);
}

void AdaptiveDifferentPrioStrategy::onBufferSent(QueryId, uint64_t)
{
}

}
