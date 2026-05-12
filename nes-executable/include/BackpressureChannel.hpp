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
#include <cstdint>
#include <memory>
#include <stop_token>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Priority.hpp>
#include <QueryId.hpp>

namespace NES
{
struct BackpressureStatisticListener;
class AdaptiveSendingScheduler;
}

struct Channel;
class BackpressureListener;
class BackpressureController;

/// Per-channel atomic state shared between the BackpressureController (writer + reader) and the
/// AdaptiveSendingScheduler (reader + writer). Lives behind a shared_ptr so the scheduler can hold
/// a reference while a controller is being destroyed.
///
///   contingent_bytes        — Bytes the channel is permitted to send this tick. Decremented on
///                             isScheduledToSend; replenished by the scheduler each tick up to
///                             a burst cap.
///   queue_depth_bytes       — Bytes approved but not yet acknowledged on the wire. The scheduler
///                             reads this as the channel's current demand.
///   delivered_bytes_last_tick — Bytes that completed sending since the scheduler last drained
///                             this counter. The scheduler atomically exchanges it to 0 each tick
///                             to compute the operative capacity estimate.
struct ChannelSchedulerState
{
    std::atomic<uint64_t> contingent_bytes{0};
    std::atomic<uint64_t> queue_depth_bytes{0};
    std::atomic<uint64_t> delivered_bytes_last_tick{0};
};

/// This is the entrypoint to a backpressure channel. It creates a pair of connected Backpressure Controller and BackpressureListener.
/// A Backpressure Controller controls the Backpressure, and a BackpressureListener only allows further progress if there is no backpressure.
/// In NebulaStream a Backpressure Controller is owned by exactly one sink, which controls all the BackpressureListener of all sources within the same query plan.
/// Currently, the Backpressure channel enforces the invariant that sinks always outlive sources. Thus, if a Backpressure Controller is destroyed, all
/// connected BackpressureListeners that are still alive and in use will report an assertion failure.
std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();

/// A Backpressure Controller is the exclusive controller of a backpressure channel. It allows the user to apply and release backpressure, which blocks
/// or unblocks all connected Ingestions.
class BackpressureController
{
    explicit BackpressureController(std::shared_ptr<Channel> channel);

    std::shared_ptr<Channel> channel;
    std::shared_ptr<NES::BackpressureStatisticListener> statisticListener;
    NES::QueryId statQueryId = NES::QueryId::invalid();
    NES::Priority statPriority = NES::Priority::LOW;

    /// Shared atomic state with the AdaptiveSendingScheduler. Always non-null; default-constructed
    /// even when no scheduler is registered, so isScheduledToSend / recordBufferSentBytes can
    /// be called unconditionally from NetworkSink without a null check.
    std::shared_ptr<ChannelSchedulerState> schedulerState = std::make_shared<ChannelSchedulerState>();
    /// Weak ptr to the worker-wide scheduler. Set by registerWithScheduler; destructor's
    /// unregisterChannel call no-ops when the scheduler has gone away first.
    std::weak_ptr<NES::AdaptiveSendingScheduler> scheduler;
    bool schedulerRegistered = false;

    friend std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();

public:
    ~BackpressureController();

    /// Currently, a Backpressure Controller represents unique ownership over the backpressure channel, thus copying is not enabled.
    BackpressureController(const BackpressureController& other) = delete;
    BackpressureController& operator=(const BackpressureController& other) = delete;

    /// Default moves leaves channel in an empty state which prevents unintended destruction of the underlying channel
    BackpressureController(BackpressureController&& other) noexcept = default;
    BackpressureController& operator=(BackpressureController&& other) noexcept = default;

    bool applyPressure();
    bool releasePressure();

    /// Wires the controller to a statistic listener. After this is called, applyPressure / releasePressure /
    /// recordBufferSent will fire BackpressureEvents identifying *queryId* and *priority*. Called once during
    /// ExecutableQueryPlan::instantiate after the channel is created.
    void setStatisticListener(std::shared_ptr<NES::BackpressureStatisticListener> listener, NES::QueryId queryId, NES::Priority priority);

    /// Hot-path emission point invoked from NetworkSink::execute() after every successful send_buffer().
    /// No-op when no listener is wired.
    void recordBufferSent(uint64_t numberOfTuples);

    /// Register this controller's scheduler state with the worker-wide AdaptiveSendingScheduler.
    /// After registration, the scheduler tick will start setting `contingent_bytes` on this
    /// channel based on the configured priority weights and observed wire capacity. Called once
    /// during ExecutableQueryPlan::instantiate alongside setStatisticListener.
    void registerWithScheduler(std::shared_ptr<NES::AdaptiveSendingScheduler> scheduler, NES::QueryId queryId, NES::Priority priority);

    /// Approve a buffer for sending via the AdaptiveSendingScheduler's per-channel contingent.
    /// CAS-decrements `contingent_bytes` by `bufferSizeBytes` and increments `queue_depth_bytes`
    /// by the same amount so the scheduler sees this buffer as in-flight demand. Returns true on
    /// approval; on false, the NetworkSink should apply source pressure and repeat-task the buffer
    /// until the scheduler refills the contingent on its next tick.
    ///
    /// When the controller is not registered with a scheduler, this returns true unconditionally
    /// (no gating). The check is byte-precise so the WEIGHTED_PRIO strategy can express bandwidth
    /// shares independent of buffer size.
    bool isScheduledToSend(uint64_t bufferSizeBytes);

    /// Sender-side complement of isScheduledToSend. Called from NetworkSink::execute on
    /// SendResult::Ok. Decrements queue_depth_bytes (this buffer is no longer in-flight) and
    /// increments delivered_bytes_last_tick so the scheduler's next tick can update its operative
    /// capacity estimate. No-op when the controller is not registered with a scheduler.
    void recordBufferSentBytes(uint64_t bufferSizeBytes);

    /// Fire a SchedulerGatedEvent with the elapsed time of a scheduler-gating episode (one or more
    /// consecutive isScheduledToSend denials followed by a pass-through). The sink tracks the
    /// episode start in steady_clock-nanoseconds and passes the elapsed nanoseconds here on
    /// pass-through. No-op when no listener is wired.
    void recordSchedulerGated(uint64_t gatedNs);

    /// Sojourn-time tracking. Called from NetworkSink::execute at the top of every invocation
    /// (after the closed check). `try_emplace` semantics: if the buffer is already in the map
    /// from a prior gated retry, the original arrival timestamp is preserved. The triple
    /// (sequence, origin, chunk) is the wire-protocol buffer identity — defensive against
    /// multi-origin merges and split-chunk buffers. No-op when no listener is wired.
    void recordBufferArrival(NES::SequenceNumber seq, NES::OriginId origin, NES::ChunkNumber chunk);

    /// Sender-side complement of recordBufferArrival. Called from NetworkSink::execute on
    /// SendResult::Ok (after recordBufferSent). Looks up the buffer's first-arrival timestamp,
    /// computes sojourn = now - arrival, fires BufferSojournEvent, and erases the entry.
    /// No-op (and no event) if no arrival was tracked or no listener is wired.
    void recordBufferSojourn(NES::SequenceNumber seq, NES::OriginId origin, NES::ChunkNumber chunk);

    /// Cleanup hook for buffers that will never complete (e.g. on SendResult::Closed). Erases the
    /// in-flight arrival entry without emitting an event.
    void forgetBufferArrival(NES::SequenceNumber seq, NES::OriginId origin, NES::ChunkNumber chunk);
};

/// Listener of one or more backpressure channels, used by sources. Before initiating a read of a new buffer,
/// the source can check whether any controller has applied backpressure by calling `wait`. The thread blocks
/// on the call if backpressure has been applied on any of the underlying channels, until pressure is released
/// (or the stop token is signaled). When a query plan has multiple sinks, each sink owns its own controller;
/// the listener returned by `createBackpressureChannel` is composed via `merge` so sources only need to hold
/// a single listener that aggregates all sinks' backpressure signals.
class BackpressureListener
{
    explicit BackpressureListener(std::shared_ptr<Channel> channel) : channels{std::move(channel)} { }

    friend std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();
    std::vector<std::shared_ptr<Channel>> channels;
    std::shared_ptr<NES::BackpressureStatisticListener> statisticListener;
    NES::QueryId statQueryId = NES::QueryId::invalid();
    NES::Priority statPriority = NES::Priority::LOW;

public:
    void wait(const std::stop_token& stopToken) const;

    /// Append `other`'s channels into this listener. After merge, `wait()` waits on every channel; if any
    /// controller applies pressure, the source blocks until that controller releases.
    void merge(BackpressureListener other);

    /// Mirror of BackpressureController::setStatisticListener for the source side. Wired by ExecutableQueryPlan::instantiate
    /// alongside the controller so BufferIngestEvents reference the same query identity.
    void setStatisticListener(std::shared_ptr<NES::BackpressureStatisticListener> listener, NES::QueryId queryId, NES::Priority priority);

    /// Called from SourceThread on each ingested buffer. *numberOfTuples* is the buffer's tuple
    /// count (or byte count for raw-bytes sources before the InputFormatter parses them). No-op
    /// when no listener is wired.
    void recordBufferIngested(uint64_t numberOfTuples);

    /// Called from SourceThread once, at the first successful buffer produced by a staircase-
    /// pattern source (Source::isStaircaseSource() == true). Fires StaircasePhaseStartEvent so the
    /// python binner has a per-trial t=0 reference for staircase-phase alignment across trials.
    /// No-op when no listener is wired.
    void recordStaircasePhaseStart(uint32_t phaseIdx);
};
