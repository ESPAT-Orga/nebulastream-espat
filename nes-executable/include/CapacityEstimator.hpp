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
#include <CapacityEstimatorMode.hpp>

namespace NES
{

/// Strategy for computing the operative wire capacity used by the AdaptiveSendingScheduler.
///
/// Implementations are called once per scheduler tick and return the bytes/sec to distribute
/// among priority classes. Two concrete implementations live below: `EmaCapacityEstimator`
/// (adaptive) and `FixedCapacityEstimator` (constant).
class CapacityEstimator
{
public:
    virtual ~CapacityEstimator() = default;

    CapacityEstimator(const CapacityEstimator&) = delete;
    CapacityEstimator& operator=(const CapacityEstimator&) = delete;
    CapacityEstimator(CapacityEstimator&&) = delete;
    CapacityEstimator& operator=(CapacityEstimator&&) = delete;

    /// Bytes/sec capacity to allocate for the next tick. Called once per tick.
    /// *deliveredBytesLastTick* — total wire delivery observed since the previous tick,
    ///                            summed across all registered channels.
    /// *totalDemandBytes*       — sum of `queue_depth_bytes` across all registered channels.
    /// *tickSeconds*            — scheduler tick period in seconds.
    [[nodiscard]] virtual uint64_t update(uint64_t deliveredBytesLastTick, uint64_t totalDemandBytes, double tickSeconds) = 0;

    /// Current operative capacity (bytes/s) without advancing internal state. Used by tests
    /// and the scheduler's debug log.
    [[nodiscard]] virtual uint64_t current() const = 0;

protected:
    CapacityEstimator() = default;
};

/// EMA-based estimator with a "hold on low demand" rule. Updates only when total demand
/// meets or exceeds what the current estimate would allocate for the next tick — otherwise the
/// estimate would erode during quiet periods and starve channels when traffic returns.
class EmaCapacityEstimator final : public CapacityEstimator
{
public:
    /// Alpha is EMA smoothing factor, higher more reactive, lower smoother but laggier
    EmaCapacityEstimator(uint64_t bootstrapBps, double alpha) : estimateBps(bootstrapBps), alpha(alpha) { }

    [[nodiscard]] uint64_t update(uint64_t deliveredBytesLastTick, uint64_t totalDemandBytes, double tickSeconds) override
    {
        const auto currentBps = estimateBps.load(std::memory_order_relaxed);
        const auto estimateBytesPerTick = static_cast<uint64_t>(static_cast<double>(currentBps) * tickSeconds);
        if (deliveredBytesLastTick > 0 && totalDemandBytes >= estimateBytesPerTick)
        {
            const auto observedBps = static_cast<uint64_t>(static_cast<double>(deliveredBytesLastTick) / tickSeconds);
            /// Blending observed throughput with the previous estimate
            const auto blendedBps
                = static_cast<uint64_t>(alpha * static_cast<double>(observedBps) + (1.0 - alpha) * static_cast<double>(currentBps));
            estimateBps.store(blendedBps, std::memory_order_relaxed);
            return blendedBps;
        }
        return currentBps;
    }

    [[nodiscard]] uint64_t current() const override { return estimateBps.load(std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> estimateBps;
    double alpha;
};

/// Fixed-capacity estimator. Returns the configured value every tick; ignores observed
/// throughput. Use when the wire capacity is known a priori — e.g., an experimental setup with
/// a configured tc throttle — to avoid the EMA-erosion artifact where the estimate decays
/// toward the source's effective production rate when that rate sits below the wire's real cap.
class FixedCapacityEstimator final : public CapacityEstimator
{
public:
    explicit FixedCapacityEstimator(uint64_t capacityBps) : capacityBps(capacityBps) { }

    [[nodiscard]] uint64_t update(uint64_t /*deliveredBytesLastTick*/, uint64_t /*totalDemandBytes*/, double /*tickSeconds*/) override
    {
        return capacityBps;
    }

    [[nodiscard]] uint64_t current() const override { return capacityBps; }

private:
    uint64_t capacityBps;
};

}
