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
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include <folly/Synchronized.h>
#include <BackpressureChannel.hpp>
#include <CapacityEstimator.hpp>
#include <Priority.hpp>
#include <QueryId.hpp>
#include <Thread.hpp>

namespace NES
{

/// Per-worker bandwidth-aware HTB-style scheduler for NetworkSink send arbitration.
///
/// Each tick (default 100 ms) the scheduler:
///   1. Drains delivered_bytes_last_tick across registered channels, optionally updates the EMA
///      capacity estimate.
///   2. Computes a provisional per-class budget from capacity × weight × tickDuration.
///   3. Splits each class's budget equally among demanding channels (queue_depth_bytes > 0).
///   4. Redistributes unused share in descending priority order to classes with unmet demand.
///   5. Accumulates each channel's share into its contingent_bytes atomic up to a burst cap.
///
/// NetworkSink consults BackpressureController::isScheduledToSend(bufferSizeBytes) before every
/// Rust send_buffer call. The contingent gating gives HIGH a guaranteed share, gives LOW a
/// liveness floor, and is work-conserving — total wire utilization tends to the operative
/// capacity estimate when at least one class has demand.
///
/// References:
///   * Hierarchical Token Bucket (HTB), Devera — https://luxik.cdi.cz/~devik/qos/htb/
///   * Weighted Fair Queueing / GPS — https://en.wikipedia.org/wiki/Weighted_fair_queueing
///   * DiffServ Assured Forwarding — RFC 2597, https://www.rfc-editor.org/rfc/rfc2597
class AdaptiveSendingScheduler
{
public:
    struct SchedulerConfig
    {
        std::chrono::milliseconds tickPeriod{100};
        /// MUST stay highest-priority-first: Phase 4 redistributes leftover bandwidth by walking
        /// this map in order and giving slack to the first non-empty class, so a wrong order
        /// breaks priority. std::map<Priority> already sorts by the enum, so keep HIGH < LOW.
        std::map<Priority, double> priorityWeights{{Priority::HIGH, 0.8}, {Priority::LOW, 0.2}};
        uint64_t burstCapPerChannelBytes = 32 * 1024;
        bool debugLog = false;
        /// Strategy for computing the operative wire capacity each tick. Must be set by the
        /// caller (typically NodeEngineBuilder constructs an EmaCapacityEstimator or a
        /// FixedCapacityEstimator based on worker config). Shared because tests / callers may
        /// keep their own reference to inspect the estimator's state.
        std::shared_ptr<CapacityEstimator> capacityEstimator;
    };

    explicit AdaptiveSendingScheduler(SchedulerConfig schedulerConfig);

    /// Stop the background thread; safe to call multiple times.
    ~AdaptiveSendingScheduler();

    AdaptiveSendingScheduler(const AdaptiveSendingScheduler&) = delete;
    AdaptiveSendingScheduler& operator=(const AdaptiveSendingScheduler&) = delete;

    /// Start the periodic tick thread. Call once after construction. Safe to call again — second
    /// call is a no-op when already running.
    void start();

    /// Register a channel's atomic state under (queryId, priority). The scheduler will start
    /// applying per-tick contingent allocation to this channel on the next tick. shared_ptr so
    /// the atomics survive controller destruction in the middle of a tick.
    void registerChannel(QueryId queryId, Priority priority, std::shared_ptr<ChannelSchedulerState> state);

    /// Remove a previously registered channel. Idempotent; safe to call from the controller
    /// destructor even if registration never completed.
    void unregisterChannel(const QueryId& queryId, Priority priority);

    /// Run one allocation pass synchronously on the calling thread — i.e. without the background
    /// tick thread. Exposed for unit tests that drive the scheduler deterministically (no sleep,
    /// no concurrency).
    void assignContingents();

    /// Current operative capacity estimate in bytes per second. Exposed for tests.
    [[nodiscard]] uint64_t capacityEstimateBps() const { return config.capacityEstimator ? config.capacityEstimator->current() : 0; }

private:
    struct RegisteredChannel
    {
        QueryId queryId;
        Priority priority;
        std::shared_ptr<ChannelSchedulerState> state;
    };

    void threadRoutine(const std::stop_token& stopToken);

    SchedulerConfig config;
    /// Channel registry. Read-locked during a tick (allows many concurrent registrations to
    /// queue behind the tick), write-locked during register/unregister.
    folly::Synchronized<std::vector<RegisteredChannel>> channels;
    Thread schedulerThread;
    std::atomic<bool> started{false};
};

}
