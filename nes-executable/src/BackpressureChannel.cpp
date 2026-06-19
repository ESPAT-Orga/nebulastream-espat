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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <unordered_map>
#include <utility>

#include <folly/Synchronized.h>

#include <AdaptiveSendingScheduler.hpp>
#include <BackpressureStatisticsListener.hpp>
#include <ErrorHandling.hpp>

namespace
{
/// Wire-protocol identity of a TupleBuffer along a channel: (sequence, origin, chunk). Used as the
/// key into the per-channel arrival-time map. Stored as raw u64 values so we don't need to
/// specialize std::hash for the NESStrongType wrappers.
struct BufferKey
{
    uint64_t seq;
    uint64_t origin;
    uint64_t chunk;

    bool operator==(const BufferKey& other) const noexcept { return seq == other.seq && origin == other.origin && chunk == other.chunk; }
};
}

template <>
struct std::hash<BufferKey>
{
    size_t operator()(const BufferKey& k) const noexcept
    {
        /// Cheap mixing — collisions inside a single channel are extremely unlikely (the wire
        /// protocol relies on the triple being unique), so a simple xor-shift mix is fine.
        size_t h = std::hash<uint64_t>{}(k.seq);
        h ^= std::hash<uint64_t>{}(k.origin) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= std::hash<uint64_t>{}(k.chunk) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return h;
    }
};

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

    /// Per-buffer first-arrival timestamps for sojourn-time tracking. Inserted by
    /// `BackpressureController::recordBufferArrival` on every `NetworkSink::execute` entry
    /// (try_emplace → idempotent across retries); erased by `recordBufferSojourn` on send-
    /// completion and by `forgetBufferArrival` on the Closed branch. Bounded by in-flight
    /// buffer count.
    folly::Synchronized<std::unordered_map<BufferKey, std::chrono::steady_clock::time_point>, std::mutex> arrivalTimes;
};

BackpressureController::BackpressureController(std::shared_ptr<Channel> channel) : channel{std::move(channel)}
{
}

BackpressureController::~BackpressureController()
{
    /// Unregister from the scheduler *first* so its periodic tick stops touching this channel's
    /// atomics before the underlying Channel is destroyed.
    if (schedulerRegistered)
    {
        if (auto sched = scheduler.lock())
        {
            sched->unregisterChannel(statQueryId, statPriority);
        }
        schedulerRegistered = false;
    }
    if (channel)
    {
        *channel->stateMtx.lock() = Channel::DESTROYED;
        channel->change.notify_all();
    }
}

bool BackpressureController::applyPressure()
{
    const auto old = std::exchange(*channel->stateMtx.lock(), Channel::CLOSED);
    INVARIANT(old != Channel::DESTROYED, "The backpressureController is still alive thus the channel should not have been destroyed");
    const bool transitioned = old == Channel::OPEN;
    if (transitioned && statisticListener)
    {
        statisticListener->onEvent(NES::ApplyPressureEvent{statQueryId, statPriority});
    }
    return transitioned;
}

bool BackpressureController::releasePressure()
{
    const auto old = std::exchange(*channel->stateMtx.lock(), Channel::OPEN);
    INVARIANT(old != Channel::DESTROYED, "The Backpressure Controller is still alive thus the channel should not have been destroyed");
    if (old == Channel::CLOSED)
    {
        /// The Backpressure Controller was opened, wake up all waiting BackpressureListeners
        channel->change.notify_all();
        if (statisticListener)
        {
            statisticListener->onEvent(NES::ReleasePressureEvent{statQueryId, statPriority});
        }
        return true;
    }
    return false;
}

void BackpressureController::setStatisticListener(
    std::shared_ptr<NES::BackpressureStatisticListener> listener, NES::QueryId queryId, NES::Priority priority)
{
    statisticListener = std::move(listener);
    statQueryId = std::move(queryId);
    statPriority = priority;
}

void BackpressureController::recordBufferSent(uint64_t numberOfTuples)
{
    if (statisticListener)
    {
        statisticListener->onEvent(NES::BufferSentEvent{statQueryId, statPriority, numberOfTuples});
    }
}

void BackpressureController::registerWithScheduler(
    std::shared_ptr<NES::AdaptiveSendingScheduler> sched, NES::QueryId queryId, NES::Priority priority)
{
    if (!sched)
    {
        return;
    }
    /// Stash identity so the destructor's unregisterChannel call has the right keys.
    statQueryId = std::move(queryId);
    statPriority = priority;
    scheduler = sched;
    sched->registerChannel(statQueryId, statPriority, schedulerState);
    schedulerRegistered = true;
}

bool BackpressureController::isScheduledToSend(uint64_t bufferSizeBytes)
{
    /// Only the Weighted send path calls this, and Weighted is only selected when a scheduler is
    /// configured — so reaching here unregistered is an internal inconsistency, not a runtime case.
    INVARIANT(schedulerRegistered, "isScheduledToSend called without a registered scheduler");
    auto current = schedulerState->contingent_bytes.load(std::memory_order_acquire);
    while (current >= bufferSizeBytes)
    {
        if (schedulerState->contingent_bytes.compare_exchange_weak(
                current, current - bufferSizeBytes, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            schedulerState->queue_depth_bytes.fetch_add(bufferSizeBytes, std::memory_order_relaxed);
            return true;
        }
        /// compare_exchange_weak refreshes `current` on failure; loop and retry.
    }
    return false;
}

void BackpressureController::recordBufferSentBytes(uint64_t bufferSizeBytes)
{
    if (!schedulerRegistered)
    {
        return;
    }
    schedulerState->queue_depth_bytes.fetch_sub(bufferSizeBytes, std::memory_order_relaxed);
    schedulerState->delivered_bytes_last_tick.fetch_add(bufferSizeBytes, std::memory_order_relaxed);
}

void BackpressureController::recordSchedulerGated(uint64_t gatedNs)
{
    if (!statisticListener)
    {
        return;
    }
    statisticListener->onEvent(NES::SchedulerGatedEvent{statQueryId, statPriority, gatedNs});
}

void BackpressureController::recordBufferArrival(NES::SequenceNumber seq, NES::OriginId origin, NES::ChunkNumber chunk)
{
    if (!statisticListener)
    {
        return;
    }
    const BufferKey key{seq.getRawValue(), origin.getRawValue(), chunk.getRawValue()};
    channel->arrivalTimes.lock()->try_emplace(key, std::chrono::steady_clock::now());
}

void BackpressureController::recordBufferSojourn(NES::SequenceNumber seq, NES::OriginId origin, NES::ChunkNumber chunk)
{
    if (!statisticListener)
    {
        return;
    }
    const BufferKey key{seq.getRawValue(), origin.getRawValue(), chunk.getRawValue()};
    std::chrono::steady_clock::time_point arrival;
    {
        auto guard = channel->arrivalTimes.lock();
        const auto it = guard->find(key);
        if (it == guard->end())
        {
            /// No arrival was tracked (probably because the listener was wired up mid-flight).
            /// Skip emit rather than reporting a bogus sojourn.
            return;
        }
        arrival = it->second;
        guard->erase(it);
    }
    const auto sojournNs
        = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - arrival).count());
    statisticListener->onEvent(NES::BufferSojournEvent{statQueryId, statPriority, sojournNs});
}

void BackpressureController::forgetBufferArrival(NES::SequenceNumber seq, NES::OriginId origin, NES::ChunkNumber chunk)
{
    if (!statisticListener)
    {
        return;
    }
    const BufferKey key{seq.getRawValue(), origin.getRawValue(), chunk.getRawValue()};
    channel->arrivalTimes.lock()->erase(key);
}

void BackpressureListener::wait(const std::stop_token& stopToken) const
{
    /// Captured inside the locked scope so blockedNs reflects only time spent inside
    /// channel->change.wait — not the subsequent lock-release / INVARIANT-check overhead.
    /// Sub-microsecond difference vs sampling after the scope, but it's the more honest place.
    std::chrono::steady_clock::time_point blockStart;
    std::chrono::steady_clock::time_point blockEnd;
    /// Block while ANY channel is closed; resume when all are open (or stop is requested).
    for (const auto& channel : channels)
    {
        if (stopToken.stop_requested())
        {
            return;
        }
        auto state = channel->stateMtx.lock();
        if (*state == Channel::State::OPEN)
        {
            continue;
        }

        blockStart = std::chrono::steady_clock::now();
        bool destroyed = false;
        channel->change.wait(
            state.as_lock(),
            stopToken,
            [&destroyed, &state] -> bool
            {
                destroyed = *state == Channel::DESTROYED;
                return destroyed || *state == Channel::OPEN;
            });
        blockEnd = std::chrono::steady_clock::now();


        INVARIANT(!destroyed, "Backpressure Controller was destroyed before the BackpressureListener");
    }

    /// Block-time accounting (lock released): emit how long the source thread spent blocked.
    /// Captures the upstream side of backpressure that the sojourn metric is blind to — under
    /// ALWAYS_SEND with a saturated wire, most of the per-tuple wait shows up here rather than
    /// at the sink. Only emitted when statisticListener is wired (mirrors the recordBuffer* hot
    /// paths on the controller side).
    if (statisticListener)
    {
        const auto blockedNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(blockEnd - blockStart).count());
        statisticListener->onEvent(NES::BackpressureBlockedEvent{statQueryId, statPriority, blockedNs});
    }
}

void BackpressureListener::merge(BackpressureListener other)
{
    for (auto& channel : other.channels)
    {
        channels.push_back(std::move(channel));
    }
}

void BackpressureListener::setStatisticListener(
    std::shared_ptr<NES::BackpressureStatisticListener> listener, NES::QueryId queryId, NES::Priority priority)
{
    statisticListener = std::move(listener);
    statQueryId = std::move(queryId);
    statPriority = priority;
}

void BackpressureListener::recordBufferIngested(uint64_t numberOfTuples)
{
    if (statisticListener)
    {
        statisticListener->onEvent(NES::BufferIngestEvent{statQueryId, statPriority, numberOfTuples});
    }
}

void BackpressureListener::recordStaircasePhaseStart(uint32_t phaseIdx)
{
    if (statisticListener)
    {
        statisticListener->onEvent(NES::StaircasePhaseStartEvent{statQueryId, statPriority, phaseIdx});
    }
}

std::pair<BackpressureController, BackpressureListener> createBackpressureChannel()
{
    const auto channel = std::make_shared<Channel>();
    return {BackpressureController{channel}, BackpressureListener{channel}};
}
