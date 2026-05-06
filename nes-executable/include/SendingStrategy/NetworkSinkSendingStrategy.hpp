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

/// Decides whether a query is currently allowed to send a buffer through its NetworkSink.
///
/// Lifetime: a single instance per worker, owned by the NodeEngine. NetworkSinks register and deregister their
/// channels here as queries start/stop. The strategy is consulted on the NetworkSink hot path via maySend(),
/// and informed about backpressure events and successful sends through the on... hooks.
///
/// Implementations must be safe to call from multiple threads concurrently.
class NetworkSinkSendingStrategy
{
public:
    NetworkSinkSendingStrategy() = default;
    virtual ~NetworkSinkSendingStrategy() = default;

    /// The strategy holds worker-shared state (e.g. AdaptiveDifferentPrioStrategy's channels map and
    /// backpressure counter) and is referenced concurrently from every NetworkSink on the worker via a
    /// shared_ptr held by the NodeEngine. Copying would split the state into two unsynchronised instances;
    /// moving would invalidate references held by sinks that still observe the original. The single instance
    /// must remain pinned for its lifetime, so all four operations are deleted.
    NetworkSinkSendingStrategy(const NetworkSinkSendingStrategy&) = delete;
    NetworkSinkSendingStrategy& operator=(const NetworkSinkSendingStrategy&) = delete;
    NetworkSinkSendingStrategy(NetworkSinkSendingStrategy&&) = delete;
    NetworkSinkSendingStrategy& operator=(NetworkSinkSendingStrategy&&) = delete;

    /// Called once per NetworkSink before the first send. Idempotent if called for an already-registered query.
    virtual void registerChannel(QueryId queryId, Priority priority) = 0;

    /// Called when the NetworkSink is torn down. Removes the channel from the strategy's bookkeeping.
    virtual void deregisterChannel(QueryId queryId) = 0;

    /// Hot-path gate consulted by NetworkSink::execute() before each send.
    /// Returns true if the query may attempt to send the next buffer; false if the query should be buffered.
    [[nodiscard]] virtual bool maySend(QueryId queryId) const = 0;

    /// Called the first time backpressure is acquired for the given query (i.e. when the underlying network
    /// channel reports it cannot accept more data). LOW-priority backpressure may be ignored; HIGH-priority
    /// backpressure is the signal that gates LOW-priority queries in the adaptive strategy.
    virtual void onBackpressureApplied(QueryId queryId) = 0;

    /// Called when backpressure is released for the given query (the buffer queue drained below the lower threshold).
    virtual void onBackpressureReleased(QueryId queryId) = 0;

    /// Called after a buffer has been successfully sent. Strategies may use this for accounting or metrics.
    virtual void onBufferSent(QueryId queryId, uint64_t numTuples) = 0;
};

}
