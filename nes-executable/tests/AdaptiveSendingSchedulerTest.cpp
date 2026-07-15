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

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <CapacityEstimator.hpp>
#include <Priority.hpp>
#include <QueryId.hpp>

namespace NES
{

namespace
{
/// Build a deterministic SchedulerConfig that drives the allocator without spinning the thread.
/// tickPeriod = 100ms, capacity = 1 mbit/s = 125000 B/s → 12500 B per tick. Weights HIGH=0.8 /
/// LOW=0.2 → HIGH share 10000 B, LOW share 2500 B per tick. Burst cap 32 KB.
AdaptiveSendingScheduler::SchedulerConfig makeConfig(double emaAlpha = 0.3)
{
    return AdaptiveSendingScheduler::SchedulerConfig{
        .tickPeriod = std::chrono::milliseconds{100},
        .priorityWeights = {{Priority::HIGH, 0.8}, {Priority::LOW, 0.2}},
        .burstCapPerChannelBytes = 32 * 1024,
        .debugLog = false,
        .capacityEstimator = std::make_shared<EmaCapacityEstimator>(/*bootstrap=*/125'000U, emaAlpha),
    };
}

AdaptiveSendingScheduler::SchedulerConfig makeConfigFixed(uint64_t capacityBps)
{
    return AdaptiveSendingScheduler::SchedulerConfig{
        .tickPeriod = std::chrono::milliseconds{100},
        .priorityWeights = {{Priority::HIGH, 0.8}, {Priority::LOW, 0.2}},
        .burstCapPerChannelBytes = 32 * 1024,
        .debugLog = false,
        .capacityEstimator = std::make_shared<FixedCapacityEstimator>(capacityBps),
    };
}

/// Create a fresh QueryId for a test channel. QueryId requires UUID-based construction;
/// numeric-literal-style construction is not supported.
QueryId makeQueryId()
{
    return QueryId::createLocal(LocalQueryId{generateUUID()});
}

/// Convenience: register a channel with a given priority and an initial demand (queue_depth_bytes).
std::shared_ptr<ChannelSchedulerState>
registerWithDemand(AdaptiveSendingScheduler& scheduler, QueryId queryId, Priority priority, uint64_t demandBytes)
{
    auto state = std::make_shared<ChannelSchedulerState>();
    state->queue_depth_bytes.store(demandBytes);
    scheduler.registerChannel(std::move(queryId), priority, state);
    return state;
}

constexpr uint64_t HIGH_SHARE_PER_TICK = 10'000; /// 0.8 × 125000 B/s × 0.1 s
constexpr uint64_t LOW_SHARE_PER_TICK = 2'500; ///  0.2 × 125000 B/s × 0.1 s
constexpr uint64_t BUDGET_PER_TICK = HIGH_SHARE_PER_TICK + LOW_SHARE_PER_TICK; /// 12500
} /// namespace

class AdaptiveSendingSchedulerTest : public ::testing::Test
{
protected:
    void SetUp() override { Logger::setupLogging("AdaptiveSendingSchedulerTest.log", NES::LogLevel::LOG_DEBUG); }
};

/// Test 1: Single HIGH below capacity → contingent = HIGH share + LOW slack (LOW class empty).
/// The scheduler no longer caps the grant at the channel's queue_depth_bytes (demand). Token-
/// bucket burst cap prevents unbounded accumulation across ticks.
TEST_F(AdaptiveSendingSchedulerTest, SingleHighBelowCapacity)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, /*demandBytes=*/3'000);
    scheduler.assignContingents();
    /// LOW class is empty → its 2500-byte share is slack, flows to HIGH (the only non-empty class).
    EXPECT_EQ(high->contingent_bytes.load(), BUDGET_PER_TICK);
}

/// Test 2: Single HIGH at capacity → contingent = full budget (LOW class is empty, residual flows).
TEST_F(AdaptiveSendingSchedulerTest, SingleHighSaturating)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    scheduler.assignContingents();
    /// HIGH gets its 0.8 share + LOW's idle 0.2 share = full budget per tick.
    EXPECT_EQ(high->contingent_bytes.load(), BUDGET_PER_TICK);
}

/// Test 3: HIGH + LOW both saturating → 0.8 / 0.2 split.
TEST_F(AdaptiveSendingSchedulerTest, HighAndLowBothSaturating)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    auto low = registerWithDemand(scheduler, makeQueryId(), Priority::LOW, BUDGET_PER_TICK * 2);
    scheduler.assignContingents();
    EXPECT_EQ(high->contingent_bytes.load(), HIGH_SHARE_PER_TICK);
    EXPECT_EQ(low->contingent_bytes.load(), LOW_SHARE_PER_TICK);
}

/// Test 4: HIGH below share, LOW saturating → both still get their per-class share.
/// Without within-class slack redistribution, HIGH's unused share (queue_depth_bytes < share)
/// stays in HIGH's contingent (accumulates up to burst cap). LOW does not pull HIGH's slack
/// because neither class is empty.
TEST_F(AdaptiveSendingSchedulerTest, HighBelowShareLowGetsClassShareOnly)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, /*demand=*/2'000);
    auto low = registerWithDemand(scheduler, makeQueryId(), Priority::LOW, BUDGET_PER_TICK * 2);
    scheduler.assignContingents();
    EXPECT_EQ(high->contingent_bytes.load(), HIGH_SHARE_PER_TICK);
    EXPECT_EQ(low->contingent_bytes.load(), LOW_SHARE_PER_TICK);
}

/// Test 5: HIGH saturating, LOW partial demand → both get their per-class share.
/// Mirror of Test 4: LOW's unused share is not redistributed within-tick. HIGH gets exactly its
/// share regardless of LOW's small demand.
TEST_F(AdaptiveSendingSchedulerTest, HighSaturatesLowPartial)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    auto low = registerWithDemand(scheduler, makeQueryId(), Priority::LOW, /*demand=*/1'000);
    scheduler.assignContingents();
    EXPECT_EQ(high->contingent_bytes.load(), HIGH_SHARE_PER_TICK);
    EXPECT_EQ(low->contingent_bytes.load(), LOW_SHARE_PER_TICK);
}

/// Test 6: Two HIGH channels saturating → each gets 0.4 (HIGH share split equally).
TEST_F(AdaptiveSendingSchedulerTest, TwoHighChannelsSplitEqually)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    auto high1 = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    auto high2 = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    auto low = registerWithDemand(scheduler, makeQueryId(), Priority::LOW, BUDGET_PER_TICK * 2);
    scheduler.assignContingents();
    /// HIGH share 10000 / 2 channels = 5000 each.
    EXPECT_EQ(high1->contingent_bytes.load(), HIGH_SHARE_PER_TICK / 2);
    EXPECT_EQ(high2->contingent_bytes.load(), HIGH_SHARE_PER_TICK / 2);
    EXPECT_EQ(low->contingent_bytes.load(), LOW_SHARE_PER_TICK);
}

/// Test 7: Channel registers mid-run → first tick before registration only sees the prior channel.
TEST_F(AdaptiveSendingSchedulerTest, ChannelRegistersMidRun)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    scheduler.assignContingents();
    EXPECT_EQ(high->contingent_bytes.load(), BUDGET_PER_TICK); /// sole channel gets all budget

    auto low = registerWithDemand(scheduler, makeQueryId(), Priority::LOW, BUDGET_PER_TICK * 2);
    /// Set HIGH's contingent back to 0 to isolate this tick's allocation.
    high->contingent_bytes.store(0);
    scheduler.assignContingents();
    EXPECT_EQ(high->contingent_bytes.load(), HIGH_SHARE_PER_TICK);
    EXPECT_EQ(low->contingent_bytes.load(), LOW_SHARE_PER_TICK);
}

/// Test 8: Channel deregisters mid-tick → scheduler doesn't allocate to it; doesn't crash.
TEST_F(AdaptiveSendingSchedulerTest, ChannelDeregistersMidRun)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    const auto lowQueryId = makeQueryId();
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    auto low = registerWithDemand(scheduler, lowQueryId, Priority::LOW, BUDGET_PER_TICK * 2);
    scheduler.assignContingents();
    EXPECT_EQ(low->contingent_bytes.load(), LOW_SHARE_PER_TICK);

    /// Deregister LOW; reset HIGH; tick again.
    scheduler.unregisterChannel(lowQueryId, Priority::LOW);
    high->contingent_bytes.store(0);
    /// LOW's atomic state still alive (we hold the shared_ptr), but scheduler no longer touches it.
    const auto lowBefore = low->contingent_bytes.load();
    scheduler.assignContingents();
    EXPECT_EQ(high->contingent_bytes.load(), BUDGET_PER_TICK); /// HIGH again sole channel
    EXPECT_EQ(low->contingent_bytes.load(), lowBefore); /// untouched
}

/// Test 9: Token-bucket accumulation. With share-per-tick (10 KB) just above buffer size (8 KB),
/// two ticks at zero starting contingent should accumulate to ≥ buffer size both times.
TEST_F(AdaptiveSendingSchedulerTest, TokenBucketAccumulates)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    scheduler.assignContingents();
    EXPECT_EQ(high->contingent_bytes.load(), BUDGET_PER_TICK); /// 12500 (single channel, gets full budget)
    /// Simulate sending a buffer: deduct 8192 from contingent.
    high->contingent_bytes.fetch_sub(8192);
    EXPECT_EQ(high->contingent_bytes.load(), BUDGET_PER_TICK - 8192);
    /// Next tick: contingent += 12500 (capped at burst 32768).
    scheduler.assignContingents();
    EXPECT_EQ(high->contingent_bytes.load(), std::min<uint64_t>(BUDGET_PER_TICK - 8192 + BUDGET_PER_TICK, 32768));
}

/// Test 10: Burst cap. Idle channel doesn't accumulate beyond burst cap.
TEST_F(AdaptiveSendingSchedulerTest, BurstCapBoundsAccumulation)
{
    auto config = makeConfig();
    config.burstCapPerChannelBytes = 20'000;
    AdaptiveSendingScheduler scheduler(config);
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    for (int i = 0; i < 10; ++i)
    {
        scheduler.assignContingents();
    }
    /// After 10 ticks of 12500 each, would be 125000 without cap; capped at 20000.
    EXPECT_EQ(high->contingent_bytes.load(), 20'000U);
}

/// Test 11: EMA convergence. Feed delivered-bytes such that observed = bootstrap; estimate holds.
TEST_F(AdaptiveSendingSchedulerTest, EmaHoldsWithObservedAtBootstrap)
{
    AdaptiveSendingScheduler scheduler(makeConfig(/*emaAlpha=*/0.3));
    const uint64_t initial = scheduler.capacityEstimateBps();
    EXPECT_EQ(initial, 125'000U);

    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    /// Pretend that this tick delivered exactly budget_per_tick bytes.
    high->delivered_bytes_last_tick.store(BUDGET_PER_TICK);
    scheduler.assignContingents();
    /// EMA should leave capacity near 125k bps (within EMA tolerance).
    EXPECT_NEAR(static_cast<double>(scheduler.capacityEstimateBps()), 125'000.0, 1'000.0);
}

/// Test 12: EMA holds when traffic is well below the capacity estimate (no erosion during quiet
/// periods). This is the rationale for the "only update when total_demand >= estimate × tick" rule.
TEST_F(AdaptiveSendingSchedulerTest, EmaHoldsOnLowDemand)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    const uint64_t initial = scheduler.capacityEstimateBps();
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, /*demand=*/100);
    /// Pretend we delivered only 100 bytes this tick — total demand 100 < estimate × tick (12500).
    high->delivered_bytes_last_tick.store(100);
    scheduler.assignContingents();
    /// Estimate should be unchanged (hold).
    EXPECT_EQ(scheduler.capacityEstimateBps(), initial);
}

/// Test 13: Bootstrap before first observation. delivered_bytes_last_tick = 0 → estimate stays at
/// bootstrap.
TEST_F(AdaptiveSendingSchedulerTest, BootstrapUsedBeforeFirstObservation)
{
    AdaptiveSendingScheduler scheduler(makeConfig());
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 2);
    /// delivered_bytes_last_tick is 0 by default; assignContingents should not update estimate.
    scheduler.assignContingents();
    EXPECT_EQ(scheduler.capacityEstimateBps(), 125'000U);
    /// And HIGH still gets allocated based on the bootstrap value (full budget — sole channel).
    EXPECT_EQ(high->contingent_bytes.load(), BUDGET_PER_TICK);
}

/// Test 14: FixedCapacityEstimator ignores delivered_bytes_last_tick. Regardless of what the
/// channels report as delivered, the operative capacity stays at the configured value across
/// many ticks. Verifies that the abstraction wires through correctly and the EMA hold-rule
/// can be sidestepped by callers that know their wire capacity.
TEST_F(AdaptiveSendingSchedulerTest, FixedCapacityEstimatorIgnoresDeliveredBytes)
{
    /// Configure a fixed estimator at 200_000 B/s (different from EMA defaults to avoid coincidence).
    AdaptiveSendingScheduler scheduler(makeConfigFixed(/*capacityBps=*/200'000U));
    auto high = registerWithDemand(scheduler, makeQueryId(), Priority::HIGH, BUDGET_PER_TICK * 10);
    EXPECT_EQ(scheduler.capacityEstimateBps(), 200'000U);

    /// Simulate the worst-case "observed << capacity" scenario that would cause EMA erosion:
    /// pretend each tick we delivered only 1000 bytes. Fixed estimator should hold.
    for (int i = 0; i < 50; ++i)
    {
        high->delivered_bytes_last_tick.store(1'000);
        high->contingent_bytes.store(0); /// simulate "send burned all contingent"
        scheduler.assignContingents();
        EXPECT_EQ(scheduler.capacityEstimateBps(), 200'000U);
    }
    /// Per-tick budget under fixed capacity: 200000 × 0.1 = 20000 bytes. HIGH sole channel of
    /// HIGH class + LOW slack (LOW class empty) → HIGH gets full 20000 per tick, capped at burst.
    EXPECT_EQ(high->contingent_bytes.load(), std::min<uint64_t>(20'000U, 32U * 1024U));
}

} /// namespace NES
