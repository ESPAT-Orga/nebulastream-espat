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
#include <stop_token>
#include <string>
#include <utility>
#include <variant>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <boost/asio.hpp>
#include <fmt/ostream.h>
#include <folly/MPMCQueue.h>
#include <scope_guard.hpp>

#include "AdaptiveSendingScheduler.hpp"

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

BackpressureStatisticTcpEmitter::BackpressureStatisticTcpEmitter(std::string hostAddress, uint16_t port)
    : hostAddress(std::move(hostAddress)), port(port)
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

    try
    {
        asio::io_context io;

        tcp::endpoint endpoint(asio::ip::make_address(hostAddress), port);
        tcp::acceptor acceptor(io);
        acceptor.open(endpoint.protocol());
        acceptor.set_option(tcp::acceptor::reuse_address(true));
        acceptor.bind(endpoint);
        acceptor.listen();

        NES_INFO("Listening on {}:{}", hostAddress, port);

        tcp::socket socket(io);
        /// This will accept exactly one client. If another one comes afterwards it will never connect
        acceptor.accept(socket); /// blocks until a client connects

        boost::system::error_code ec;
        while (!token.stop_requested())
        {
            BackpressureEvent event = ApplyPressureEvent{INVALID_LOCAL_QUERY_ID, INVALID_PRIORITY}; /// Will be overwritten

            if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(READ_RETRY_MS), event))
            {
                continue;
            }
            std::string msg;

            std::visit(
                Overloaded{
                    [&](const UnbufferingCompletedEvent& unbufferEvent)
                    {
                        //TODO: implement
                        (void) unbufferEvent;
                    },
                    [&](const BufferSentEvent& sentEvent)
                    {
                        auto nanosec
                            = std::chrono::duration_cast<std::chrono::nanoseconds>(sentEvent.timestamp.time_since_epoch()).count();
                        NES_TRACE("Sent event for {}, {}", sentEvent.localQueryId, sentEvent.timestamp);
                        msg = std::format("BufferSent,{},{}\n", sentEvent.localQueryId.getRawValue(), nanosec);
                    },
                        [&](const ApplyPressureEvent& applyEvent)
                    {
                        auto nanosec
                            = std::chrono::duration_cast<std::chrono::nanoseconds>(applyEvent.timestamp.time_since_epoch()).count();
                        NES_TRACE("Apply Backpressure {}, {}", applyEvent.localQueryId, applyEvent.timestamp);
                        msg = std::format("ApplyPressure,{},{}\n", applyEvent.localQueryId.getRawValue(), nanosec);
                    },
                    [&](const ReleasePressureEvent& releaseEvent)
                    {
                        auto nanosec
                            = std::chrono::duration_cast<std::chrono::nanoseconds>(releaseEvent.timestamp.time_since_epoch()).count();
                        NES_TRACE("Release Backpressure {}, {}", releaseEvent.localQueryId, releaseEvent.timestamp);
                        msg = std::format("ReleasePressure,{},{}\n", releaseEvent.localQueryId.getRawValue(), nanosec);
                    }},
                event);

            asio::write(socket, asio::buffer(msg), ec);
            if (ec)
            {
                NES_ERROR("Error sending message: {}", ec.message());
            }
        }

        boost::system::error_code ignored;
        socket.shutdown(tcp::socket::shutdown_both, ignored);
        socket.close(ignored);
    }
    catch (const std::exception& e)
    {
        NES_ERROR("server error: {}", e.what());
    }
}

}
