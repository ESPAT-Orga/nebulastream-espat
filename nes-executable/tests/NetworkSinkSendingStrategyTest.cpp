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
#include <SendingStrategy/AlwaysSendStrategy.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategy.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategyFactory.hpp>
#include <SendingStrategy/WeightedPriorityStrategy.hpp>
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
    return QueryId::createLocal(LocalQueryId{localUuid});
}
}

class NetworkSinkSendingStrategyTest : public ::testing::Test
{
protected:
    void SetUp() override { Logger::setupLogging("NetworkSinkSendingStrategyTest.log", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(NetworkSinkSendingStrategyTest, AlwaysSendDispatchesToDirect)
{
    /// AlwaysSendStrategy returns SendVariant::Direct for every channel, regardless of priority,
    /// pressure events, or whether the channel was registered. Event hooks are no-ops.
    AlwaysSendStrategy strategy;
    const auto qHigh = makeQueryId("00000000-0000-0000-0000-000000000001");
    const auto qLow = makeQueryId("00000000-0000-0000-0000-000000000002");
    const auto qUnregistered = makeQueryId("00000000-0000-0000-0000-000000000099");

    strategy.registerChannel(qHigh, Priority::HIGH);
    strategy.registerChannel(qLow, Priority::LOW);

    EXPECT_EQ(strategy.sendVariant(qHigh), SendVariant::Direct);
    EXPECT_EQ(strategy.sendVariant(qLow), SendVariant::Direct);
    EXPECT_EQ(strategy.sendVariant(qUnregistered), SendVariant::Direct);

    /// Event hooks must be no-ops and not affect the dispatch.
    strategy.onBackpressureApplied(qHigh);
    strategy.onBackpressureReleased(qHigh);
    strategy.onBufferSent(qHigh, 100);
    EXPECT_EQ(strategy.sendVariant(qHigh), SendVariant::Direct);
}

TEST_F(NetworkSinkSendingStrategyTest, WeightedDispatchesToWeighted)
{
    /// WeightedPriorityStrategy is a thin dispatcher: it always returns SendVariant::Weighted.
    /// The actual gate (consult per-channel contingent from the AdaptiveSendingScheduler) lives
    /// in NetworkSink::execute and is exercised by integration tests / the network-sink
    /// benchmark, not here.
    WeightedPriorityStrategy strategy;
    const auto qHigh = makeQueryId("00000000-0000-0000-0000-000000000001");
    const auto qLow = makeQueryId("00000000-0000-0000-0000-000000000002");
    const auto qUnregistered = makeQueryId("00000000-0000-0000-0000-000000000099");

    strategy.registerChannel(qHigh, Priority::HIGH);
    strategy.registerChannel(qLow, Priority::LOW);

    EXPECT_EQ(strategy.sendVariant(qHigh), SendVariant::Weighted);
    EXPECT_EQ(strategy.sendVariant(qLow), SendVariant::Weighted);
    EXPECT_EQ(strategy.sendVariant(qUnregistered), SendVariant::Weighted);

    /// Event hooks must be no-ops and not affect the dispatch.
    strategy.onBackpressureApplied(qHigh);
    strategy.onBackpressureReleased(qHigh);
    strategy.onBufferSent(qHigh, 100);
    EXPECT_EQ(strategy.sendVariant(qHigh), SendVariant::Weighted);
}

TEST_F(NetworkSinkSendingStrategyTest, FactoryProducesSelectedImplementation)
{
    const auto alwaysSend = createNetworkSinkSendingStrategy(NetworkSinkSendingStrategyType::ALWAYS_SEND);
    const auto weighted = createNetworkSinkSendingStrategy(NetworkSinkSendingStrategyType::WEIGHTED_PRIO);

    ASSERT_NE(alwaysSend, nullptr);
    ASSERT_NE(weighted, nullptr);
    EXPECT_NE(dynamic_cast<AlwaysSendStrategy*>(alwaysSend.get()), nullptr);
    EXPECT_NE(dynamic_cast<WeightedPriorityStrategy*>(weighted.get()), nullptr);
    EXPECT_EQ(alwaysSend->sendVariant(makeQueryId("00000000-0000-0000-0000-000000000001")), SendVariant::Direct);
    EXPECT_EQ(weighted->sendVariant(makeQueryId("00000000-0000-0000-0000-000000000001")), SendVariant::Weighted);
}

}
