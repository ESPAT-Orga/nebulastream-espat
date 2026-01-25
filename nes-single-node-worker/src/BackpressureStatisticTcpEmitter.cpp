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

#include <BackpressureStatisticTcpEmitter.hpp>
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
#include <boost/asio.hpp>

namespace NES
{

constexpr uint64_t READ_RETRY_MS = 100;
constexpr uint64_t SYSTEM_THREAD = 0;
/// Log every nth dropped event to avoid clogging the log when the queue is full
constexpr uint64_t DROP_LOG_INTERVAL = 100;

namespace
{
void warnOnOverflow(bool writeFailed)
{
    if (writeFailed) [[unlikely]]
    {
        static std::atomic<uint64_t> droppedCount{0};
        /// Log first drop immediately, then every DROP_LOG_INTERVAL
        if (uint64_t dropped = droppedCount.fetch_add(1, std::memory_order_relaxed) + 1; dropped == 1 || dropped % DROP_LOG_INTERVAL == 0)
        {
            NES_WARNING("Event queue full, {} events dropped so far", dropped);
        }
    }
}
}

void BackpressureStatisticTcpEmitter::onEvent(const BackpressureEvent event)
{
    warnOnOverflow(
        !events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return BackpressureEvent(std::forward<T>(arg)); }, std::move(event))));
}

BackpressureStatisticTcpEmitter::BackpressureStatisticTcpEmitter()
{
    NES_INFO("Creating backpressure statistics source");
}

void BackpressureStatisticTcpEmitter::start()
{
    tcpWriterThread = Thread("backpressure-tcp-writer", [this](const std::stop_token& stopToken) { threadRoutine(stopToken); });
}

void BackpressureStatisticTcpEmitter::threadRoutine(const std::stop_token& token)
{
    namespace asio = boost::asio;
    using asio::ip::tcp;

    //TODO: get from config
    // const std::string host = "127.0.0.1";
    const std::string host = "host.docker.internal";
    const std::string port = "9000";

    asio::io_context io;
    tcp::resolver resolver(io);

    // Resolve once; for localhost this is fine. If you want to handle DNS changes,
    // move resolve() inside the loop.
    auto endpoints = resolver.resolve(host, port);

    tcp::socket socket(io);

    for (;;) {
        boost::system::error_code ec;

        socket.close(ec); // ignore close errors
        socket = tcp::socket(io);

        asio::connect(socket, endpoints, ec);
        if (!ec) {
            NES_INFO("Backpressure statistics tcp emitter connected to {}:{}", host, port);
            break;
        }

        NES_INFO("Connection to {}:{} failed, with {} retrying in {}", host, port, ec.message(), CONNECT_RETRY_INTERVAL);
        std::this_thread::sleep_for(CONNECT_RETRY_INTERVAL);
    }


    boost::system::error_code ec;
    while (!token.stop_requested())
    {
        BackpressureEvent event = ApplyPressureEvent{"INVALID"}; /// Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(READ_RETRY_MS), event))
        {
            continue;
        }
        std::string msg;

        std::visit(
            Overloaded{
                [&](const ApplyPressureEvent& applyEvent)
                {
                    NES_INFO("Apply Backpressure {}, {}", applyEvent.channelId, applyEvent.timestamp);
                    msg = std::string{"APPLY"};
                },
                [&](const ReleasePressureEvent& releaseEvent)
                {
                    NES_INFO("Release Backpressure {}, {}", releaseEvent.channelId, releaseEvent.timestamp);
                    msg = std::string{"RELEASE"};
                }},
            event);

        asio::write(socket, asio::buffer(msg), ec);
        if (ec) {
            NES_ERROR("Error sending message: {}", ec.message());
        }
    }

    socket.close(ec);
}

}
