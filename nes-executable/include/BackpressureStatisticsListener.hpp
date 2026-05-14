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
#include <cstdint>
#include <variant>
#include <Priority.hpp>
#include <QueryId.hpp>

namespace NES
{
using ChronoClock = std::chrono::system_clock;

/// Common fields recorded on every BackpressureEvent. Identifies which query the event belongs to and
/// when it occurred. Uses the full QueryId (local UUID + distributed name) instead of a bare
/// LocalQueryId so emitted events line up with the existing throughput/latency listener log format.
struct BaseBackpressureEvent
{
    BaseBackpressureEvent(QueryId queryId, Priority priority) : queryId(std::move(queryId)), priority(priority) { }

    BaseBackpressureEvent() = default;

    ChronoClock::time_point timestamp = ChronoClock::now();
    QueryId queryId = QueryId::invalid();
    Priority priority = Priority::LOW;
};

/// Fired when the BackpressureController transitions from OPEN -> CLOSED, blocking source threads.
struct ApplyPressureEvent : BaseBackpressureEvent
{
    ApplyPressureEvent(QueryId queryId, Priority priority) : BaseBackpressureEvent(std::move(queryId), priority) { }

    ApplyPressureEvent() = default;
};

/// Fired when the BackpressureController transitions back to OPEN, unblocking sources.
struct ReleasePressureEvent : BaseBackpressureEvent
{
    ReleasePressureEvent(QueryId queryId, Priority priority) : BaseBackpressureEvent(std::move(queryId), priority) { }

    ReleasePressureEvent() = default;
};

/// Fired by NetworkSink each time send_buffer() returns Ok — i.e. one TupleBuffer has been accepted by
/// the wire (post-tc, post-kernel-throttle). numberOfTuples is the buffer's tuple count.
struct BufferSentEvent : BaseBackpressureEvent
{
    BufferSentEvent(QueryId queryId, Priority priority, uint64_t numberOfTuples)
        : BaseBackpressureEvent(std::move(queryId), priority), numberOfTuples(numberOfTuples)
    {
    }

    BufferSentEvent() = default;

    uint64_t numberOfTuples = 0;
};

/// Fired by SourceThread each time the source ingests a buffer. numberOfTuples is the buffer's tuple
/// count at ingest time so the bench can sum it directly into a delivered-tuples-per-second metric
/// without having to multiply by an across-trial average buffer fill (which over-reports bursty
/// traffic).
struct BufferIngestEvent : BaseBackpressureEvent
{
    BufferIngestEvent(QueryId queryId, Priority priority, uint64_t numberOfTuples)
        : BaseBackpressureEvent(std::move(queryId), priority), numberOfTuples(numberOfTuples)
    {
    }

    BufferIngestEvent() = default;

    uint64_t numberOfTuples = 0;
};

/// Fired by NetworkSink::execute alongside BufferSentEvent on every successful send. sojournNs is
/// the elapsed time from when the buffer FIRST entered NetworkSink::execute (recorded on arrival
/// via BackpressureController::recordBufferArrival) to when the send_buffer FFI call returned Ok
/// (recordBufferSojourn). Captures engine-side queueing delay — backpressure waits + scheduler-gate
/// retries + Full-induced retries — which is the metric a priority scheduler actually affects.
/// Does NOT include Rust mpsc dequeue or TCP wire time (send_buffer returns at enqueue).
struct BufferSojournEvent : BaseBackpressureEvent
{
    BufferSojournEvent(QueryId queryId, Priority priority, uint64_t sojournNs)
        : BaseBackpressureEvent(std::move(queryId), priority), sojournNs(sojournNs)
    {
    }

    BufferSojournEvent() = default;

    uint64_t sojournNs = 0;
};

/// Fired by BackpressureListener::wait() each time the source thread returns from a CLOSED-state
/// block (channel reopens or is destroyed). blockedNs is the wall-clock duration the source spent
/// blocked. Captures the upstream side of backpressure that the sojourn metric is blind to —
/// under ALWAYS_SEND with a saturated wire, most of the per-tuple wait shows up here rather than
/// at the sink. The event's `timestamp` is the wait-end (system_clock), so an offline consumer can
/// reconstruct the wait window via start = end - blockedNs.
struct BackpressureBlockedEvent : BaseBackpressureEvent
{
    BackpressureBlockedEvent(QueryId queryId, Priority priority, uint64_t blockedNs)
        : BaseBackpressureEvent(std::move(queryId), priority), blockedNs(blockedNs)
    {
    }

    BackpressureBlockedEvent() = default;

    uint64_t blockedNs = 0;
};

/// Fired by NetworkSink when a buffer that was previously denied by the AdaptiveSendingScheduler's
/// per-channel contingent gate finally passes the gate (isScheduledToSend returns true). gatedNs is
/// the wall-clock duration the buffer spent in the deny/retry loop. Distinct from
/// BackpressureBlockedEvent (source-side wait on a CLOSED channel): scheduler gating happens at the
/// sink before the source ever calls wait(), so under WEIGHTED_STRICT permanently-gated LOW
/// produces this event but emits zero blocked events. The `timestamp` is the pass-through moment so
/// an offline consumer reconstructs the gated window via start = end - gatedNs.
struct SchedulerGatedEvent : BaseBackpressureEvent
{
    SchedulerGatedEvent(QueryId queryId, Priority priority, uint64_t gatedNs)
        : BaseBackpressureEvent(std::move(queryId), priority), gatedNs(gatedNs)
    {
    }

    SchedulerGatedEvent() = default;

    uint64_t gatedNs = 0;
};

/// Fired once per (queryId, priority) by SourceThread when a staircase-pattern source (e.g.
/// GeneratorSource backed by StepGeneratorRate) produces its first buffer. An offline consumer
/// uses the event timestamp as the trial's t=0 reference so HIGH-alone and contended HIGH
/// trials align on the staircase phase regardless of warmup delay. `phaseIdx=0` for the first
/// emission; reserved for future periodic re-anchoring.
struct StaircasePhaseStartEvent : BaseBackpressureEvent
{
    StaircasePhaseStartEvent(QueryId queryId, Priority priority, uint32_t phaseIdx)
        : BaseBackpressureEvent(std::move(queryId), priority), phaseIdx(phaseIdx)
    {
    }

    StaircasePhaseStartEvent() = default;

    uint32_t phaseIdx = 0;
};

using BackpressureEvent = std::variant<
    ApplyPressureEvent,
    ReleasePressureEvent,
    BufferSentEvent,
    BufferIngestEvent,
    BufferSojournEvent,
    BackpressureBlockedEvent,
    SchedulerGatedEvent,
    StaircasePhaseStartEvent>;

/// Sink-side hook invoked from worker threads — implementations must be thread-safe and non-blocking.
/// Single-node-worker provides BackpressureStatisticStdoutEmitter; an offline consumer parses its stdout lines.
struct BackpressureStatisticListener
{
    virtual ~BackpressureStatisticListener() = default;
    virtual void onEvent(BackpressureEvent event) = 0;
};

}
