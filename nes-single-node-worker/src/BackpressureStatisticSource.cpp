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

#include <BackpressureStatisticSource.hpp>
#include <BackpressureStatisticsListener.hpp>

#include <GoogleEventTracePrinter.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <stop_token>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <fmt/ostream.h>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>
#include <scope_guard.hpp>

namespace NES
{

constexpr uint64_t READ_RETRY_MS = 100;
constexpr uint64_t SYSTEM_THREAD = 0;
/// Log every nth dropped event to avoid clogging the log when the queue is full
constexpr uint64_t DROP_LOG_INTERVAL = 100;

void BackpressureStatisticSource::onEvent(const BackpressureEvent event)
{
        std::visit(
            Overloaded{
                [&](const ApplyPressureEvent& applyEvent)
                {
                    NES_INFO("Apply Backpressure {}, {}", applyEvent.channelId, applyEvent.timestamp);
                },
                [&](const ReleasePressureEvent& releaseEvent)
                {
                    NES_INFO("Release Backpressure {}, {}", releaseEvent.channelId, releaseEvent.timestamp);
                }},
            event);
}

BackpressureStatisticSource::BackpressureStatisticSource()
{
    NES_INFO("Creating backpressure statistics source");
}
}
