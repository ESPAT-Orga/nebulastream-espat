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

#include <BackpressureChannel.hpp>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stop_token>
#include <utility>

#include <folly/Synchronized.h>

#include <AdaptiveSendingScheduler.hpp>
#include <ErrorHandling.hpp>

/// Represents the state of the backpressure channel guarded by a mutex and communicated to the listener via the condition variable.
/// The channel is initially open.
struct Channel
{
    enum State : uint8_t
    {
        OPEN,
        CLOSED,
        DESTROYED,
    };

    folly::Synchronized<State, std::mutex> stateMtx{OPEN};
    std::condition_variable_any change;
    std::atomic<bool> blockedByAdaptiveSending{false};
};

BackpressureController::BackpressureController(std::shared_ptr<Channel> channel) : channel{std::move(channel)}
{
}

BackpressureController::BackpressureController(
    std::shared_ptr<Channel> channel, std::shared_ptr<NES::BackpressureStatisticListener> backpressureStatisticListener)
    : BackpressureController(std::move(channel))
{
    this->backpressureStatisticListener = std::move(backpressureStatisticListener);
}

BackpressureController::~BackpressureController()
{
    if (channel)
    {
        *channel->stateMtx.lock() = Channel::DESTROYED;
        channel->change.notify_all();
    }
}

bool BackpressureController::applyPressure(bool adaptivelyThrottled)
{
    const auto old = std::exchange(*channel->stateMtx.lock(), Channel::CLOSED);
    INVARIANT(old != Channel::DESTROYED, "The backpressureController is still alive thus the channel should not have been destroyed");

    bool newPressure = old == Channel::OPEN;
    if (!adaptivelyThrottled && backpressureStatisticListener && newPressure)
    {
        backpressureStatisticListener->onEvent(NES::ApplyPressureEvent(localQueryId, priority));
    }
    return newPressure;
}

bool BackpressureController::isScheduledToSend([[maybe_unused]] const std::string& channelId)
{
    NES_DEBUG("Checking if channel {} with priority {} is scheduled to send: blocked = {} ", channelId, priority, channel->blockedByAdaptiveSending.load());
    return !adaptiveSendingScheduler || !channel->blockedByAdaptiveSending.load();
}

bool BackpressureController::releasePressure(bool adaptivelyThrottled)
{
    const auto old = std::exchange(*channel->stateMtx.lock(), Channel::OPEN);
    INVARIANT(old != Channel::DESTROYED, "The Backpressure Controller is still alive thus the channel should not have been destroyed");
    if (old == Channel::CLOSED)
    {
        /// The Backpressure Controller was opened, wake up all waiting BackpressureListeners
        channel->change.notify_all();

        if (!adaptivelyThrottled && backpressureStatisticListener)
        {
            backpressureStatisticListener->onEvent(NES::ReleasePressureEvent(localQueryId, priority));
        }
        return true;
    }
    return false;
}

bool BackpressureController::unbufferingCompleted()
{
    const auto old = std::exchange(*channel->stateMtx.lock(), Channel::OPEN);
    INVARIANT(old != Channel::DESTROYED, "The Backpressure Controller is still alive thus the channel should not have been destroyed");
    channel->change.notify_all();

    if (backpressureStatisticListener)
    {
        backpressureStatisticListener->onEvent(NES::UnbufferingCompletedEvent(localQueryId, priority));
    }
    return true;
}

void BackpressureController::setStatisticListener(std::shared_ptr<NES::BackpressureStatisticListener> listener)
{
    this->backpressureStatisticListener = listener;
}

void BackpressureController::setAdaptiveSendingScheduler(NES::LocalQueryId localQueryId, NES::Priority priority, std::shared_ptr<NES::AdaptiveSendingScheduler> scheduler)
{
    this->adaptiveSendingScheduler = scheduler;
    this->localQueryId = localQueryId;
    NES_DEBUG("Registering channel id = {}, with priority {}", localQueryId, priority);
    // INVARIANT(priority != NES::INVALID_PRIORITY, "Priority must be valid");
    this->priority = priority;
    if (adaptiveSendingScheduler)
    {
        this->priority = adaptiveSendingScheduler->registerChannel(localQueryId, priority, channel->blockedByAdaptiveSending);
    }
}

void BackpressureListener::wait(const std::stop_token& stopToken) const
{
    auto state = channel->stateMtx.lock();
    /// If the channel is open, backpressureListener can proceed
    if (*state == Channel::State::OPEN)
    {
        return;
    }

    bool destroyed = false;
    /// Wait for the channel state to change
    channel->change.wait(
        state.as_lock(),
        stopToken,
        [&destroyed, &state] -> bool
        {
            destroyed = *state == Channel::DESTROYED;
            return destroyed || *state == Channel::OPEN;
        });

    INVARIANT(!destroyed, "Backpressure Controller was destroyed before the BackpressureListener");
}

std::pair<BackpressureController, BackpressureListener> createBackpressureChannel()
{
    const auto channel = std::make_shared<Channel>();
    return {BackpressureController{channel}, BackpressureListener{channel}};
}
