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

#include <chrono>
#include <filesystem>
#include <stop_token>
#include <variant>
#include <folly/MPMCQueue.h>
#include <BackpressureStatisticsListener.hpp>
#include <Thread.hpp>

namespace NES
{
/// This class emits collected backpressure statistics via tcp
struct BackpressureStatisticTcpEmitter final : BackpressureStatisticListener
{
    void onEvent(BackpressureEvent event) override;

    explicit BackpressureStatisticTcpEmitter(std::string hostAddress, uint16_t port);
    void start();
    void threadRoutine(const std::stop_token& token);
    ~BackpressureStatisticTcpEmitter() override = default;

private:
    static constexpr size_t QUEUE_LENGTH = 1000;
    static constexpr std::chrono::milliseconds CONNECT_RETRY_INTERVAL = std::chrono::milliseconds{500};
    const std::string hostAddress;
    const uint16_t port;
    Thread tcpWriterThread;
    folly::MPMCQueue<BackpressureEvent> events{QUEUE_LENGTH};
};
}
