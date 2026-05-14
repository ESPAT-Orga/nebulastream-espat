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

#include <atomic>
#include <barrier>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <stop_token>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BackpressureStatisticsListener.hpp>
#include <BaseUnitTest.hpp>
#include <QueryId.hpp>

namespace NES
{

class BackpressureChannelTest : public ::testing::Test
{
protected:
    void SetUp() override { Logger::setupLogging("BackpressureChannelTest.log", NES::LogLevel::LOG_DEBUG); }
};

/// Test basic construction and destruction of Backpressure Controller and BackpressureListener
TEST_F(BackpressureChannelTest, BasicConstruction)
{
    /// Test that we can create a backpressure channel
    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Test that the objects are functional by using their public methods
    /// Initially, the channel should be open (no backpressure)
    EXPECT_TRUE(backpressureController.applyPressure()); /// Should return true (was open)
    EXPECT_TRUE(backpressureController.releasePressure()); /// Should return true (was closed)
}

/// Test basic functionality with 1 Backpressure Controller and 1 backpressureListener
TEST_F(BackpressureChannelTest, BasicFunctionality)
{
    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Initially, the channel should be open (no backpressure)
    /// We can't directly test the internal state, but we can test the behavior

    /// Apply pressure - should return true (was open)
    EXPECT_TRUE(backpressureController.applyPressure());

    /// Apply pressure again - should return false (was already closed)
    EXPECT_FALSE(backpressureController.applyPressure());

    /// Release pressure - should return true (was closed)
    EXPECT_TRUE(backpressureController.releasePressure());

    /// Release pressure again - should return false (was already open)
    EXPECT_FALSE(backpressureController.releasePressure());
}

/// Test that backpressureListener proceeds immediately when no pressure is applied
TEST_F(BackpressureChannelTest, BackpressureListenerProceedsWhenNoPressure)
{
    std::barrier syncBarrier{2};
    std::atomic backpressureListenerCounter{0};

    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Start backpressureListener without applying pressure
    std::jthread backpressureListenerThread(
        [&](const std::stop_token& stopToken)
        {
            syncBarrier.arrive_and_wait();
            while (!stopToken.stop_requested())
            {
                backpressureListener.wait(stopToken);
                backpressureListenerCounter.fetch_add(1, std::memory_order::relaxed);
            }
        });

    syncBarrier.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    backpressureListenerThread = {};

    /// This is a guess, however 100 sounds very doable on any real hardware
    EXPECT_GT(backpressureListenerCounter.load(), 100);
}

/// Test that backpressureListener is blocked when pressure is applied
TEST_F(BackpressureChannelTest, BackpressureListenerProceedsWithPressure)
{
    std::barrier syncBarrier{2};
    std::atomic backpressureListenerCounter{0};

    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Start backpressureListener without applying pressure
    std::jthread backpressureListenerThread(
        [&](const std::stop_token& stopToken)
        {
            syncBarrier.arrive_and_wait();
            while (!stopToken.stop_requested())
            {
                backpressureListener.wait(stopToken);
                backpressureListenerCounter.fetch_add(1, std::memory_order::relaxed);
            }
        });

    syncBarrier.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    /// This is a guess, however 100 sounds very doable on any real hardware
    EXPECT_GT(backpressureListenerCounter.load(), 100);
    EXPECT_TRUE(backpressureController.applyPressure());

    /// Expect that the backpressureListener does not increase any further after pressure has been applied
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto current = backpressureListenerCounter.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(current, backpressureListenerCounter.load());
}

/// Test that backpressureListener waits when pressure is applied
TEST_F(BackpressureChannelTest, IngestionWaitsWhenPressureApplied)
{
    constexpr size_t numberOfSources = 5;
    /// Read on main thread only happens after the backpressureListenerThreads have been stopped.
    std::vector<std::chrono::milliseconds> durations(numberOfSources);

    std::barrier syncBeforeWait{numberOfSources + 1};
    std::barrier syncAfterWait{numberOfSources + 1};

    auto [backpressureController, backpressureListener] = createBackpressureChannel();

    /// Apply pressure before starting backpressureListener
    backpressureController.applyPressure();

    std::vector<std::jthread> backpressureListenerThreads;
    backpressureListenerThreads.reserve(numberOfSources);
    for (size_t i = 0; i < numberOfSources; ++i)
    {
        /// Start a thread that will try to ingest
        backpressureListenerThreads.emplace_back(
            [&, i](const std::stop_token& stopToken)
            {
                syncBeforeWait.arrive_and_wait();
                auto start = std::chrono::steady_clock::now();

                /// This should block until pressure is released
                backpressureListener.wait(stopToken);

                auto end = std::chrono::steady_clock::now();

                syncAfterWait.arrive_and_wait();

                /// Report time spend waiting
                durations[i] = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            });
    }
    syncBeforeWait.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    /// Release pressure
    EXPECT_TRUE(backpressureController.releasePressure());
    syncAfterWait.arrive_and_wait();

    /// Stop thread to ensure that there is no data race on duration
    backpressureListenerThreads.clear();

    /// Expect time to have passed while waiting for backpressure release. This is a guess, we cannot predict an actual duration
    for (auto duration : durations)
    {
        EXPECT_GT(duration, std::chrono::milliseconds(50));
    }
}

/// Stress test with multiple Backpressure Controllers and backpressureListeners in a multithreaded environment
TEST_F(BackpressureChannelTest, MultithreadedStressTest)
{
    constexpr int numChannels = 10;
    constexpr int numIngestionsPerChannel = 5;
    constexpr int testDurationMs = 1000;

    std::vector<std::pair<BackpressureController, BackpressureListener>> channels;
    channels.reserve(numChannels);

    /// Create multiple backpressure channels
    for (int i = 0; i < numChannels; ++i)
    {
        channels.emplace_back(createBackpressureChannel());
    }

    std::atomic totalOperations{0};
    std::atomic successfulWaits{0};

    /// Barrier to synchronize all threads
    std::barrier syncBarrier{1 + (numChannels * numIngestionsPerChannel) + numChannels};

    /// Start ingestion threads
    std::vector<std::jthread> ingestionThreads;
    for (int channelId = 0; channelId < numChannels; ++channelId)
    {
        for (int ingestionId = 0; ingestionId < numIngestionsPerChannel; ++ingestionId)
        {
            ingestionThreads.emplace_back(
                [&, channelId](const std::stop_token& stopToken)
                {
                    syncBarrier.arrive_and_wait();

                    while (!stopToken.stop_requested())
                    {
                        channels[channelId].second.wait(stopToken);
                        successfulWaits.fetch_add(1);
                    }
                });
        }
    }

    /// Start Backpressure Controller operation threads
    std::vector<std::jthread> backpressureControllerThreads;
    backpressureControllerThreads.reserve(numChannels);
    for (int channelId = 0; channelId < numChannels; ++channelId)
    {
        backpressureControllerThreads.emplace_back(
            [&, channelId](const std::stop_token& stopToken)
            {
                syncBarrier.arrive_and_wait();

                std::mt19937 rng(channelId);
                std::uniform_int_distribution<> dist(0, 1);

                while (!stopToken.stop_requested())
                {
                    if (dist(rng) == 0)
                    {
                        channels[channelId].first.applyPressure();
                    }
                    else
                    {
                        channels[channelId].first.releasePressure();
                    }

                    totalOperations.fetch_add(1);
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            });
    }


    /// Run the test for the specified duration
    syncBarrier.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(testDurationMs));
    ingestionThreads.clear();
    backpressureControllerThreads.clear();

    /// Verify we had some activity
    EXPECT_GT(totalOperations.load(), 0);
    EXPECT_GT(successfulWaits.load(), 0);

    /// All channels should still be functional
    for (int i = 0; i < numChannels; ++i)
    {
        channels[i].first.applyPressure();
        EXPECT_TRUE(channels[i].first.releasePressure());
    }
}

/// Test Backpressure Controller destruction behavior
TEST_F(BackpressureChannelTest, BackpressureControllerDestruction)
{
    SKIP_IF_TSAN();
    GTEST_FLAG_SET(death_test_style, "threadsafe");

    auto [backpressureController, ingestion] = createBackpressureChannel();

    /// Apply pressure
    EXPECT_TRUE(backpressureController.applyPressure());

    std::barrier syncBarrier{2};

    /// Backpressure Controller Thread keeps Backpressure Controller alive until barrier is reached
    const std::jthread ingestionThread(
        [&, backpressureController = std::move(backpressureController)]
        {
            syncBarrier.arrive_and_wait();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        });

    syncBarrier.arrive_and_wait();
    EXPECT_DEATH_DEBUG(ingestion.wait({}), "");
}

namespace
{
/// Mock BackpressureStatisticListener for the sojourn tests: records every event it sees so the
/// test can assert exactly which events fired in which order. Thread-safe.
struct RecordingListener : BackpressureStatisticListener
{
    void onEvent(BackpressureEvent event) override
    {
        const std::lock_guard guard(mtx);
        events.push_back(std::move(event));
    }

    std::vector<BackpressureEvent> snapshot() const
    {
        const std::lock_guard guard(mtx);
        return events;
    }

    mutable std::mutex mtx;
    std::vector<BackpressureEvent> events;
};

QueryId makeQueryIdForTest(std::string_view localUuid)
{
    return QueryId::createLocal(LocalQueryId{localUuid});
}

template <typename EventT>
size_t countEvents(const std::vector<BackpressureEvent>& events)
{
    size_t n = 0;
    for (const auto& ev : events)
    {
        if (std::holds_alternative<EventT>(ev))
        {
            ++n;
        }
    }
    return n;
}
}

/// Test A — happy path: arrival → sleep → sojourn fires exactly one BufferSojournEvent.
TEST_F(BackpressureChannelTest, SojournHappyPath)
{
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    auto listener = std::make_shared<RecordingListener>();
    backpressureController.setStatisticListener(listener, makeQueryIdForTest("00000000-0000-0000-0000-000000000001"), Priority::HIGH);

    backpressureController.recordBufferArrival(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    backpressureController.recordBufferSojourn(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});

    const auto events = listener->snapshot();
    ASSERT_EQ(countEvents<BufferSojournEvent>(events), 1U);
    const auto& ev = std::get<BufferSojournEvent>(events.front());
    EXPECT_GE(ev.sojournNs, 1'000'000U);
    EXPECT_LT(ev.sojournNs, 1'000'000'000U);
    EXPECT_EQ(ev.priority, Priority::HIGH);
}

/// Test B — idempotent arrival: two arrivals (simulating a pec.repeatTask retry) → only the first
/// timestamp is preserved. Sojourn reflects time from the FIRST arrival.
TEST_F(BackpressureChannelTest, SojournIdempotentArrival)
{
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    auto listener = std::make_shared<RecordingListener>();
    backpressureController.setStatisticListener(listener, makeQueryIdForTest("00000000-0000-0000-0000-000000000001"), Priority::HIGH);

    backpressureController.recordBufferArrival(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});
    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    /// Second arrival (simulating gated retry) — must NOT reset the first-arrival timestamp.
    backpressureController.recordBufferArrival(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});
    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    backpressureController.recordBufferSojourn(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});

    const auto events = listener->snapshot();
    ASSERT_EQ(countEvents<BufferSojournEvent>(events), 1U);
    const auto& ev = std::get<BufferSojournEvent>(events.front());
    /// Total elapsed since first arrival is ~6ms; if the second arrival had reset the timestamp it
    /// would only be ~3ms. Verify the larger window.
    EXPECT_GE(ev.sojournNs, 5'000'000U);
}

/// Test C — forget path: an arrival that gets forgotten produces no event on subsequent sojourn.
TEST_F(BackpressureChannelTest, SojournForgetDropsEntry)
{
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    auto listener = std::make_shared<RecordingListener>();
    backpressureController.setStatisticListener(listener, makeQueryIdForTest("00000000-0000-0000-0000-000000000001"), Priority::HIGH);

    backpressureController.recordBufferArrival(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});
    backpressureController.forgetBufferArrival(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});
    backpressureController.recordBufferSojourn(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});

    EXPECT_EQ(countEvents<BufferSojournEvent>(listener->snapshot()), 0U);
}

/// Test D — multiple keys: arrivals for several buffers; only the one we sojourn fires an event,
/// the others stay in the map.
TEST_F(BackpressureChannelTest, SojournIsolatesByKey)
{
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    auto listener = std::make_shared<RecordingListener>();
    backpressureController.setStatisticListener(listener, makeQueryIdForTest("00000000-0000-0000-0000-000000000001"), Priority::HIGH);

    backpressureController.recordBufferArrival(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});
    backpressureController.recordBufferArrival(SequenceNumber{1}, OriginId{1}, ChunkNumber{2});
    backpressureController.recordBufferArrival(SequenceNumber{2}, OriginId{1}, ChunkNumber{1});

    backpressureController.recordBufferSojourn(SequenceNumber{1}, OriginId{1}, ChunkNumber{2});

    /// Exactly one BufferSojournEvent fired, for (1, 1, 2). The other two arrivals remain tracked.
    EXPECT_EQ(countEvents<BufferSojournEvent>(listener->snapshot()), 1U);

    /// And we can still sojourn the other two correctly later.
    backpressureController.recordBufferSojourn(SequenceNumber{1}, OriginId{1}, ChunkNumber{1});
    backpressureController.recordBufferSojourn(SequenceNumber{2}, OriginId{1}, ChunkNumber{1});
    EXPECT_EQ(countEvents<BufferSojournEvent>(listener->snapshot()), 3U);
}

/// Test E — BackpressureBlocked happy path: a wait() that returns due to releasePressure emits
/// exactly one BackpressureBlockedEvent with a duration that matches the elapsed wall time.
TEST_F(BackpressureChannelTest, BackpressureBlockedFiresWhenReleased)
{
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    auto listener = std::make_shared<RecordingListener>();
    /// The block event is emitted by BackpressureListener::wait(), so the listener-side stat
    /// hook needs the same query identity.
    backpressureController.setStatisticListener(listener, makeQueryIdForTest("00000000-0000-0000-0000-000000000001"), Priority::HIGH);
    backpressureListener.setStatisticListener(listener, makeQueryIdForTest("00000000-0000-0000-0000-000000000001"), Priority::HIGH);

    backpressureController.applyPressure();

    std::barrier syncBarrier{2};
    std::jthread waiter(
        [&](const std::stop_token& stopToken)
        {
            syncBarrier.arrive_and_wait();
            backpressureListener.wait(stopToken);
        });
    syncBarrier.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    backpressureController.releasePressure();
    waiter.join();

    const auto events = listener->snapshot();
    ASSERT_EQ(countEvents<BackpressureBlockedEvent>(events), 1U);
    const auto& ev = std::get<BackpressureBlockedEvent>(events.back());
    EXPECT_GE(ev.blockedNs, 40'000'000U); /// ≥40ms — leaves headroom on slow CI
    EXPECT_LT(ev.blockedNs, 5'000'000'000U);
    EXPECT_EQ(ev.priority, Priority::HIGH);
}

/// Test F — BackpressureBlocked no-op when channel is OPEN: wait() returns immediately and
/// must not emit a BackpressureBlockedEvent (we only want events when the source was actually
/// blocked, not for every poll of a healthy channel).
TEST_F(BackpressureChannelTest, BackpressureBlockedSkippedWhenOpen)
{
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    auto listener = std::make_shared<RecordingListener>();
    backpressureListener.setStatisticListener(listener, makeQueryIdForTest("00000000-0000-0000-0000-000000000001"), Priority::HIGH);

    /// No applyPressure — channel stays OPEN; wait() returns immediately.
    std::jthread waiter([&](const std::stop_token& stopToken) { backpressureListener.wait(stopToken); });
    waiter.join();

    EXPECT_EQ(countEvents<BackpressureBlockedEvent>(listener->snapshot()), 0U);
}

/// Test stop token functionality
TEST_F(BackpressureChannelTest, StopTokenFunctionality)
{
    auto [backpressureController, ingestion] = createBackpressureChannel();

    /// Apply pressure
    EXPECT_TRUE(backpressureController.applyPressure());

    std::atomic ingestionStarted{false};
    std::atomic ingestionStopped{false};

    /// Start ingestion thread
    std::jthread ingestionThread(
        [&](const std::stop_token& stopToken)
        {
            ingestionStarted = true;
            ingestion.wait(stopToken);
            ingestionStopped = true;
        });

    /// Wait for ingestion to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(ingestionStarted);
    EXPECT_FALSE(ingestionStopped);

    /// Stop thread, should trigger stop token
    ingestionThread = {};
    EXPECT_TRUE(ingestionStopped);
    EXPECT_TRUE(backpressureController.releasePressure());
}

}
