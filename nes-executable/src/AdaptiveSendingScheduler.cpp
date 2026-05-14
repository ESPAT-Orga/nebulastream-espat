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

#include <algorithm>
#include <chrono>
#include <map>
#include <thread>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Util/Logger/Logger.hpp>

namespace NES
{

AdaptiveSendingScheduler::AdaptiveSendingScheduler(SchedulerConfig schedulerConfig) : config(std::move(schedulerConfig))
{
    PRECONDITION(config.capacityEstimator != nullptr, "AdaptiveSendingScheduler requires a non-null CapacityEstimator");
}

AdaptiveSendingScheduler::~AdaptiveSendingScheduler()
{
    /// jthread inside Thread will request_stop + join on destruction; requestStop here just sets
    /// the stop flag early. The tick sleep is not interruptible, so the routine still exits on its
    /// next loop check — up to one tickPeriod later.
    schedulerThread.requestStop();
}

void AdaptiveSendingScheduler::start()
{
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true))
    {
        return;
    }
    schedulerThread = Thread("adaptive-scheduler", [this](const std::stop_token& token) { threadRoutine(token); });
}

void AdaptiveSendingScheduler::registerChannel(QueryId queryId, Priority priority, std::shared_ptr<ChannelSchedulerState> state)
{
    PRECONDITION(state != nullptr, "AdaptiveSendingScheduler::registerChannel requires a non-null ChannelSchedulerState");
    auto locked = channels.wlock();
    locked->push_back(RegisteredChannel{.queryId = std::move(queryId), .priority = priority, .state = std::move(state)});
}

void AdaptiveSendingScheduler::unregisterChannel(const QueryId& queryId, Priority priority)
{
    auto locked = channels.wlock();
    std::erase_if(*locked, [&queryId, priority](const RegisteredChannel& c) { return c.queryId == queryId && c.priority == priority; });
}

void AdaptiveSendingScheduler::threadRoutine(const std::stop_token& token)
{
    while (!token.stop_requested())
    {
        assignContingents();
        /// Not interruptible — the stop is only observed at the next loop check, so shutdown can
        /// lag up to one tickPeriod.
        std::this_thread::sleep_for(config.tickPeriod);
    }
}

void AdaptiveSendingScheduler::assignContingents()
{
    /// Snapshot the registry under a read lock so concurrent register/unregister doesn't tear
    /// the iteration. We hold shared_ptrs to the per-channel state, so even if a controller
    /// destructs mid-tick the atomics remain alive for the duration of this function.
    auto lockedSnapshot = channels.rlock();
    if (lockedSnapshot->empty())
    {
        return;
    }
    std::vector<RegisteredChannel> channels(lockedSnapshot->begin(), lockedSnapshot->end());
    lockedSnapshot.unlock();

    const double tickSeconds = static_cast<double>(config.tickPeriod.count()) / 1000.0;

    /// Phase 1: drain per-channel delivered-bytes counters and snapshot per-channel demand (and
    /// group channel indices by priority for Phase 3). Then let the configured CapacityEstimator
    /// (EMA or fixed) decide the operative bytes/sec for this tick.
    uint64_t deliveredThisTick = 0;
    uint64_t totalDemand = 0;
    std::map<Priority, std::vector<size_t>> channelIndicesByPriority;
    std::vector<uint64_t> demandSnapshot(channels.size(), 0);
    for (size_t i = 0; i < channels.size(); ++i)
    {
        deliveredThisTick += channels[i].state->delivered_bytes_last_tick.exchange(0, std::memory_order_acq_rel);
        demandSnapshot[i] = channels[i].state->queue_depth_bytes.load(std::memory_order_relaxed);
        totalDemand += demandSnapshot[i];
        channelIndicesByPriority[channels[i].priority].push_back(i);
    }
    const auto operativeBps = config.capacityEstimator->update(deliveredThisTick, totalDemand, tickSeconds);
    const auto budgetTotal = static_cast<uint64_t>(static_cast<double>(operativeBps) * tickSeconds);

    /// Phase 2: provisional per-class budget.
    std::map<Priority, uint64_t> classBudget;
    for (const auto& [priority, weight] : config.priorityWeights)
    {
        classBudget[priority] = static_cast<uint64_t>(static_cast<double>(budgetTotal) * weight);
    }

    /// Phase 3: split each class's budget equally among ALL channels of that class.
    ///
    /// Note: we deliberately do NOT filter by demand. The earlier draft used `queue_depth_bytes`
    /// as a per-channel demand signal, but that count only grows AFTER `isScheduledToSend`
    /// returns true — which itself requires a non-zero contingent from the previous tick. So
    /// demand-based filtering at bootstrap leaves every channel at 0, never allocates any
    /// contingent, and the channel never sends. To avoid this deadlock, every registered channel
    /// of a class gets an equal slice of that class's budget. The token-bucket burst cap in
    /// Phase 5 prevents an idle channel from hoarding.
    ///
    /// Trade-off: we lose *within-class* work conservation (if one HIGH channel is idle, its
    /// share doesn't flow to the busy HIGH channel within the same tick). Across-class slack
    /// (when an entire class has zero channels) still flows in Phase 4. In the common case of one
    /// channel per priority class, within-class redistribution is a no-op anyway.
    std::vector<uint64_t> provisional(channels.size(), 0);
    uint64_t totalSlack = 0;
    for (const auto& [priority, budget] : classBudget)
    {
        const auto it = channelIndicesByPriority.find(priority);
        if (it == channelIndicesByPriority.end() || it->second.empty())
        {
            /// No registered channel in this class — whole class budget is slack.
            totalSlack += budget;
            continue;
        }
        const auto& indices = it->second;
        const auto perChannel = budget / indices.size();
        for (const auto idx : indices)
        {
            provisional[idx] = perChannel;
        }
    }

    /// Phase 4: redistribute slack from empty classes in descending priority order (HIGH first).
    /// We allocate the slack to the highest-priority class that has registered channels, splitting
    /// it equally among those channels. This is across-class work conservation only — within a
    /// class, we don't try to detect which channel "wants more" because the demand signal isn't
    /// reliable enough (see Phase 3 note).
    for (const auto& [priority, weight] : config.priorityWeights)
    {
        (void)weight;
        if (totalSlack == 0)
        {
            break;
        }
        const auto it = channelIndicesByPriority.find(priority);
        if (it == channelIndicesByPriority.end() || it->second.empty())
        {
            continue;
        }
        const auto& indices = it->second;
        const auto perChannelExtra = totalSlack / indices.size();
        for (const auto idx : indices)
        {
            provisional[idx] += perChannelExtra;
        }
        totalSlack = 0;
        break;
    }

    /// Phase 5: token-bucket accumulation. Each channel's contingent grows by its provisional
    /// share, capped at burst_cap_per_channel_bytes so an idle channel doesn't accumulate
    /// unbounded credit and burst at line rate when it next wants to send.
    for (size_t i = 0; i < channels.size(); ++i)
    {
        const auto current = channels[i].state->contingent_bytes.load(std::memory_order_relaxed);
        const auto candidate = current + provisional[i];
        const auto next = std::min<uint64_t>(candidate, config.burstCapPerChannelBytes);
        channels[i].state->contingent_bytes.store(next, std::memory_order_release);
    }

    if (config.debugLog)
    {
        std::string summary = fmt::format(
            "[scheduler tick] cap_est_bps={} delivered_bytes={} total_demand={} budget={} ",
            operativeBps,
            deliveredThisTick,
            totalDemand,
            budgetTotal);
        for (size_t i = 0; i < channels.size(); ++i)
        {
            summary += fmt::format(
                "[{} prio={} contingent={} demand={} granted={}] ",
                channels[i].queryId,
                channels[i].priority,
                channels[i].state->contingent_bytes.load(),
                demandSnapshot[i],
                provisional[i]);
        }
        NES_INFO("{}", summary);
    }
}

}
