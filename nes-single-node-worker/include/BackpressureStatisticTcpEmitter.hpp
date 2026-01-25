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
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stop_token>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/StatisticListener.hpp>
#include <BackpressureStatisticsListener.hpp>
#include <folly/MPMCQueue.h>
#include <Thread.hpp>

// #include "GoogleEventTracePrinter.hpp"

namespace NES
{
/// This printer generates Chrome DevTools trace files that can be opened in Chrome's
struct BackpressureStatisticTcpEmitter final : BackpressureStatisticListener
{
    void onEvent(BackpressureEvent event) override;

    explicit BackpressureStatisticTcpEmitter();
    void start();
    void threadRoutine(const std::stop_token& token);
    ~BackpressureStatisticTcpEmitter() override = default;

private:
    static constexpr size_t QUEUE_LENGTH = 1000;
    static constexpr std::chrono::milliseconds CONNECT_RETRY_INTERVAL = std::chrono::milliseconds{500};
    Thread tcpWriterThread;
    folly::MPMCQueue<BackpressureEvent> events{QUEUE_LENGTH};
};
}
