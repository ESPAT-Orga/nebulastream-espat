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

#include <cstdint>
#include <Priority.hpp>
#include <QueryId.hpp>

namespace NES
{

/// Selects which send path the NetworkSink should invoke for a buffer. The strategy is the
/// dispatcher — gate logic for `Weighted` lives in the C++ AdaptiveSendingScheduler consulted
/// via the BackpressureController; `Direct` skips the gate.
enum class SendVariant : uint8_t
{
    /// Call the unconditional Rust `send_buffer`. No priority gate; every buffer is queued.
    Direct,
    /// Consult `BackpressureController::isScheduledToSend(bufferSizeBytes)` first; if approved,
    /// fall through to the unconditional Rust `send_buffer`. The C++ AdaptiveSendingScheduler
    /// gives each priority class a configured share of the wire (HTB-style); empty-class slack
    /// flows to non-empty classes in priority order. LOW gets a guaranteed liveness floor when
    /// it has a non-zero weight (no starvation). Configure `scheduler_high_weight=1.0,
    /// scheduler_low_weight=0.0` for strict priority (LOW silenced, HIGH gets full wire).
    Weighted,
};

/// Strategy that picks the Rust send variant for each buffer transmission.
///
/// Lifetime: a single instance per worker, owned by the NodeEngine. NetworkSinks register and
/// deregister their channels here as queries start/stop. The strategy is consulted on the
/// NetworkSink hot path via `sendVariant()`. The `on...` hooks remain so existing
/// instrumentation (`BackpressureStatisticStdoutEmitter`) keeps receiving backpressure events,
/// even though the strategy itself no longer relies on them for its decision.
///
/// Implementations must be safe to call from multiple threads concurrently.
class NetworkSinkSendingStrategy
{
public:
    NetworkSinkSendingStrategy() = default;
    virtual ~NetworkSinkSendingStrategy() = default;

    /// The strategy holds worker-shared state and is referenced concurrently from every
    /// NetworkSink on the worker via a shared_ptr held by the NodeEngine. Copying would split
    /// the state; moving would invalidate references held by sinks. The single instance must
    /// remain pinned for its lifetime, so all four operations are deleted.
    NetworkSinkSendingStrategy(const NetworkSinkSendingStrategy&) = delete;
    NetworkSinkSendingStrategy& operator=(const NetworkSinkSendingStrategy&) = delete;
    NetworkSinkSendingStrategy(NetworkSinkSendingStrategy&&) = delete;
    NetworkSinkSendingStrategy& operator=(NetworkSinkSendingStrategy&&) = delete;

    /// Called once per NetworkSink before the first send. Idempotent if called for an
    /// already-registered query.
    virtual void registerChannel(QueryId queryId, Priority priority) = 0;

    /// Called when the NetworkSink is torn down. Removes the channel from the strategy's
    /// bookkeeping.
    virtual void deregisterChannel(QueryId queryId) = 0;

    /// Hot-path dispatcher consulted by NetworkSink::execute() before each send. Returns which
    /// send path (Direct or Weighted) this query should take — a C++ branch.
    [[nodiscard]] virtual SendVariant sendVariant(QueryId queryId) const = 0;

    /// Called when the underlying network channel returns Full. Informational only now: the
    /// gating decision lives in the C++ AdaptiveSendingScheduler (consulted via
    /// BackpressureController::isScheduledToSend).
    virtual void onBackpressureApplied(QueryId queryId) = 0;

    /// Called when backpressure is released for the given query. Informational only — feeds the
    /// instrumentation listener; the strategy makes no decision from it.
    virtual void onBackpressureReleased(QueryId queryId) = 0;

    /// Called after a buffer has been successfully sent. Informational only.
    virtual void onBufferSent(QueryId queryId, uint64_t numTuples) = 0;
};

}
