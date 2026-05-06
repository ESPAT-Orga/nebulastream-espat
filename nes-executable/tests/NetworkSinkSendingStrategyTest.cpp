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

#include <memory>
#include <string_view>
#include <SendingStrategy/AdaptiveDifferentPrioStrategy.hpp>
#include <SendingStrategy/AlwaysSendStrategy.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategy.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategyFactory.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <NetworkSinkSendingStrategyType.hpp>
#include <Priority.hpp>
#include <QueryId.hpp>

namespace NES
{

namespace
{
QueryId makeQueryId(std::string_view localUuid)
{
    return QueryId::createLocal(LocalQueryId(localUuid));
}
}

class NetworkSinkSendingStrategyTest : public ::testing::Test
{
protected:
    void SetUp() override { Logger::setupLogging("NetworkSinkSendingStrategyTest.log", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(NetworkSinkSendingStrategyTest, AlwaysSendAlwaysAllowsSending)
{
    AlwaysSendStrategy strategy;
    const auto qHigh = makeQueryId("00000000-0000-0000-0000-000000000001");
    const auto qLow = makeQueryId("00000000-0000-0000-0000-000000000002");

    strategy.registerChannel(qHigh, Priority::HIGH);
    strategy.registerChannel(qLow, Priority::LOW);

    EXPECT_TRUE(strategy.maySend(qHigh));
    EXPECT_TRUE(strategy.maySend(qLow));

    /// Reporting backpressure must not change the answer.
    strategy.onBackpressureApplied(qHigh);
    strategy.onBackpressureApplied(qLow);
    EXPECT_TRUE(strategy.maySend(qHigh));
    EXPECT_TRUE(strategy.maySend(qLow));

    strategy.onBackpressureReleased(qHigh);
    strategy.onBackpressureReleased(qLow);
    EXPECT_TRUE(strategy.maySend(qHigh));
    EXPECT_TRUE(strategy.maySend(qLow));

    /// Even unregistered queries may send.
    const auto qUnregistered = makeQueryId("00000000-0000-0000-0000-000000000099");
    EXPECT_TRUE(strategy.maySend(qUnregistered));
}

TEST_F(NetworkSinkSendingStrategyTest, AdaptiveAllowsHighAndLowWhenIdle)
{
    AdaptiveDifferentPrioStrategy strategy;
    const auto qHigh = makeQueryId("00000000-0000-0000-0000-000000000001");
    const auto qLow = makeQueryId("00000000-0000-0000-0000-000000000002");

    strategy.registerChannel(qHigh, Priority::HIGH);
    strategy.registerChannel(qLow, Priority::LOW);

    EXPECT_TRUE(strategy.maySend(qHigh));
    EXPECT_TRUE(strategy.maySend(qLow));
}

TEST_F(NetworkSinkSendingStrategyTest, AdaptivePausesLowWhileHighIsUnderBackpressure)
{
    AdaptiveDifferentPrioStrategy strategy;
    const auto qHigh = makeQueryId("00000000-0000-0000-0000-000000000001");
    const auto qLow = makeQueryId("00000000-0000-0000-0000-000000000002");

    strategy.registerChannel(qHigh, Priority::HIGH);
    strategy.registerChannel(qLow, Priority::LOW);

    strategy.onBackpressureApplied(qHigh);
    EXPECT_TRUE(strategy.maySend(qHigh)) << "HIGH-priority queries must always be allowed to send";
    EXPECT_FALSE(strategy.maySend(qLow)) << "LOW-priority queries must be paused while a HIGH channel has backpressure";

    strategy.onBackpressureReleased(qHigh);
    EXPECT_TRUE(strategy.maySend(qLow));
}

TEST_F(NetworkSinkSendingStrategyTest, AdaptiveIgnoresLowPriorityBackpressure)
{
    AdaptiveDifferentPrioStrategy strategy;
    const auto qHigh = makeQueryId("00000000-0000-0000-0000-000000000001");
    const auto qLow = makeQueryId("00000000-0000-0000-0000-000000000002");
    const auto qLowOther = makeQueryId("00000000-0000-0000-0000-000000000003");

    strategy.registerChannel(qHigh, Priority::HIGH);
    strategy.registerChannel(qLow, Priority::LOW);
    strategy.registerChannel(qLowOther, Priority::LOW);

    /// LOW backpressure events are intentionally ignored: a LOW query running into the gate is the strategy's
    /// expected behaviour, not a signal that should propagate to other LOW queries.
    strategy.onBackpressureApplied(qLow);
    EXPECT_TRUE(strategy.maySend(qLowOther)) << "LOW backpressure must not pause other LOW queries";
    EXPECT_TRUE(strategy.maySend(qHigh));
}

TEST_F(NetworkSinkSendingStrategyTest, AdaptiveCountsMultipleHighChannels)
{
    AdaptiveDifferentPrioStrategy strategy;
    const auto qHigh1 = makeQueryId("00000000-0000-0000-0000-000000000001");
    const auto qHigh2 = makeQueryId("00000000-0000-0000-0000-000000000002");
    const auto qLow = makeQueryId("00000000-0000-0000-0000-000000000003");

    strategy.registerChannel(qHigh1, Priority::HIGH);
    strategy.registerChannel(qHigh2, Priority::HIGH);
    strategy.registerChannel(qLow, Priority::LOW);

    strategy.onBackpressureApplied(qHigh1);
    strategy.onBackpressureApplied(qHigh2);
    EXPECT_FALSE(strategy.maySend(qLow));

    strategy.onBackpressureReleased(qHigh1);
    EXPECT_FALSE(strategy.maySend(qLow)) << "qHigh2 still has backpressure; LOW must remain paused";

    strategy.onBackpressureReleased(qHigh2);
    EXPECT_TRUE(strategy.maySend(qLow)) << "All HIGH channels released; LOW may send again";
}

TEST_F(NetworkSinkSendingStrategyTest, AdaptiveDoubleApplyIsIdempotent)
{
    AdaptiveDifferentPrioStrategy strategy;
    const auto qHigh = makeQueryId("00000000-0000-0000-0000-000000000001");
    const auto qLow = makeQueryId("00000000-0000-0000-0000-000000000002");

    strategy.registerChannel(qHigh, Priority::HIGH);
    strategy.registerChannel(qLow, Priority::LOW);

    strategy.onBackpressureApplied(qHigh);
    strategy.onBackpressureApplied(qHigh); /// second call must not double-count
    EXPECT_FALSE(strategy.maySend(qLow));

    strategy.onBackpressureReleased(qHigh);
    EXPECT_TRUE(strategy.maySend(qLow));
}

TEST_F(NetworkSinkSendingStrategyTest, AdaptiveDeregisterCleansUpUnderPressure)
{
    AdaptiveDifferentPrioStrategy strategy;
    const auto qHigh = makeQueryId("00000000-0000-0000-0000-000000000001");
    const auto qLow = makeQueryId("00000000-0000-0000-0000-000000000002");

    strategy.registerChannel(qHigh, Priority::HIGH);
    strategy.registerChannel(qLow, Priority::LOW);

    strategy.onBackpressureApplied(qHigh);
    EXPECT_FALSE(strategy.maySend(qLow));

    /// Deregistering a HIGH channel that was under backpressure must release the gate.
    strategy.deregisterChannel(qHigh);
    EXPECT_TRUE(strategy.maySend(qLow));
}

TEST_F(NetworkSinkSendingStrategyTest, FactoryProducesSelectedImplementation)
{
    const auto alwaysSend = createNetworkSinkSendingStrategy(NetworkSinkSendingStrategyType::ALWAYS_SEND);
    const auto adaptive = createNetworkSinkSendingStrategy(NetworkSinkSendingStrategyType::ADAPTIVE_DIFFERENT_PRIO);

    ASSERT_NE(alwaysSend, nullptr);
    ASSERT_NE(adaptive, nullptr);
    EXPECT_NE(dynamic_cast<AlwaysSendStrategy*>(alwaysSend.get()), nullptr);
    EXPECT_NE(dynamic_cast<AdaptiveDifferentPrioStrategy*>(adaptive.get()), nullptr);
}

}
