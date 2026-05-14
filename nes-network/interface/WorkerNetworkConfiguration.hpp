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

#include <memory>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <CapacityEstimatorMode.hpp>

namespace NES
{

/// Default configuration for the network layer (sender and receiver).
/// These values serve as worker-level defaults that apply to all NetworkSources and NetworkSinks on this worker.
/// Individual queries may override these defaults via per-channel configuration in the sink/source descriptors.
/// Nested under WorkerConfiguration as `worker.network.*`.
class WorkerNetworkConfiguration final : public BaseConfiguration
{
public:
    WorkerNetworkConfiguration() = default;
    WorkerNetworkConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    /// Default size of the sender software queue per network channel.
    /// May be overridden per NetworkSink via query-specific configuration.
    UIntOption senderQueueSize
        = {"sender_queue_size",
           "1024",
           "Default size of the sender software queue per network channel. May be overridden per NetworkSink.",
           {std::make_shared<NumberValidation>()}};

    /// Default maximum number of buffers that can be in-flight (sent but not yet acknowledged) per network channel.
    /// May be overridden per NetworkSink via query-specific configuration.
    UIntOption maxPendingAcks
        = {"max_pending_acks",
           "64",
           "Default maximum number of in-flight buffers awaiting acknowledgment per network channel. May be overridden per NetworkSink.",
           {std::make_shared<NumberValidation>()}};

    /// Default size of the receiver data queue per network channel.
    /// May be overridden per NetworkSource via query-specific configuration.
    UIntOption receiverQueueSize
        = {"receiver_queue_size",
           "10",
           "Default size of the receiver data queue per network channel. May be overridden per NetworkSource.",
           {std::make_shared<NumberValidation>()}};

    /// Number of IO threads for the sender tokio runtime. 0 means use the number of available cores.
    UIntOption senderIOThreads
        = {"sender_io_threads", "1", "Number of IO threads for the sender network runtime. 0 means use the number of available cores."};

    /// Number of IO threads for the receiver tokio runtime. 0 means use the number of available cores.
    UIntOption receiverIOThreads
        = {"receiver_io_threads", "1", "Number of IO threads for the receiver network runtime. 0 means use the number of available cores."};

    /// AdaptiveSendingScheduler tick period in milliseconds. The scheduler refills per-channel
    /// contingents this often. Smaller = finer-grained share, more scheduler overhead. Used only
    /// when network_sink_sending_strategy = WEIGHTED_PRIO.
    UIntOption schedulerTickMs
        = {"scheduler_tick_ms", "100", "Tick period (ms) of the AdaptiveSendingScheduler. Smaller = finer share allocation."};

    /// Per-priority weights for HTB-style share allocation. Must sum to 1.0; weights below 0 are
    /// rejected. Defaults: HIGH 0.8 / LOW 0.2 (DiffServ EF/AF style).
    FloatOption schedulerHighWeight = {"scheduler_high_weight", "0.8", "HTB weight share for HIGH-priority channels (0..1)."};
    FloatOption schedulerLowWeight = {"scheduler_low_weight", "0.2", "HTB weight share for LOW-priority channels (0..1)."};

    /// Bootstrap value for the AdaptiveSendingScheduler's online capacity estimate (bytes/sec),
    /// used until the first observed-throughput update. Set to the expected wire cap to converge
    /// from the first tick. Default: 100 Mbit/s = 12_500_000 B/s. (Stored as bytes/sec; e.g.,
    /// 1mbit ≈ 125000.) Honored only when scheduler_capacity_mode = EMA.
    UIntOption schedulerBootstrapCapacityBps
        = {"scheduler_bootstrap_capacity_bps",
           "12500000",
           "Bootstrap capacity estimate (bytes/s) used until first observed-throughput EMA update."};

    /// Selects the AdaptiveSendingScheduler's capacity-estimation strategy. EMA tracks observed
    /// throughput (and can erode under source-bp cycling); FIXED uses a constant capacity from
    /// scheduler_fixed_capacity_bps regardless of what the wire delivers.
    EnumOption<CapacityEstimatorMode> schedulerCapacityMode
        = {"scheduler_capacity_mode",
           CapacityEstimatorMode::EMA,
           "Capacity-estimation strategy for the AdaptiveSendingScheduler: EMA | FIXED"};

    /// Fixed capacity (bytes/sec) used when scheduler_capacity_mode = FIXED. The scheduler will
    /// allocate per-class shares against this constant every tick, ignoring observed throughput.
    /// Default 0 means "use scheduler_bootstrap_capacity_bps" — keeps the operator from having
    /// to set two knobs when they agree. Suitable values for the experiment: the tc cap in B/s
    /// (e.g., 125000 for 1mbit, 250000 for 2mbit).
    UIntOption schedulerFixedCapacityBps
        = {"scheduler_fixed_capacity_bps",
           "0",
           "Fixed capacity (bytes/s) when scheduler_capacity_mode = FIXED. 0 falls back to scheduler_bootstrap_capacity_bps."};

    /// Per-channel burst cap on the contingent token bucket (bytes). An idle channel cannot
    /// accumulate more than this; bursts on resume are bounded. Default: 32 KB = 4× the default
    /// 8 KB operator buffer.
    UIntOption schedulerBurstCapBytes = {
        "scheduler_burst_cap_bytes", "32768", "Per-channel burst cap (bytes) on the AdaptiveSendingScheduler's contingent token bucket."};

    /// Enable per-tick state-dump log line from the AdaptiveSendingScheduler. Useful during
    /// bring-up and calibration.
    BoolOption schedulerDebugLog
        = {"scheduler_debug_log", "false", "Log a one-line per-tick state dump from the AdaptiveSendingScheduler."};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &senderQueueSize,
            &maxPendingAcks,
            &receiverQueueSize,
            &senderIOThreads,
            &receiverIOThreads,
            &schedulerTickMs,
            &schedulerHighWeight,
            &schedulerLowWeight,
            &schedulerBootstrapCapacityBps,
            &schedulerCapacityMode,
            &schedulerFixedCapacityBps,
            &schedulerBurstCapBytes,
            &schedulerDebugLog};
    }
};
}
