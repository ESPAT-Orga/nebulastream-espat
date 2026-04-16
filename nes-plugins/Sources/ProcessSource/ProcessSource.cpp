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

#include <ProcessSource.hpp>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <format>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Files.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

ProcessSource::ProcessSource(const SourceDescriptor& sourceDescriptor)
    : pipePath(sourceDescriptor.getFromConfig(ConfigParametersProcess::PIPE_PATH))
    , pollTimeoutMs(sourceDescriptor.getFromConfig(ConfigParametersProcess::POLL_TIMEOUT_MS))
    , flushIntervalMs(sourceDescriptor.getFromConfig(ConfigParametersProcess::FLUSH_INTERVAL_MS))
{
}

void ProcessSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    const auto realPipePath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->pipePath.c_str(), nullptr), std::free};
    if (not realPipePath)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->pipePath, getErrorMessageFromERRNO());
    }
    this->fd = ::open(realPipePath.get(), O_RDONLY);
    if (this->fd < 0)
    {
        throw InvalidConfigParameter("Could not open pipe: {} - {}", this->pipePath, getErrorMessageFromERRNO());
    }
}

void ProcessSource::close()
{
    if (this->fd >= 0)
    {
        ::close(this->fd);
        this->fd = -1;
    }
}

Source::FillTupleBufferResult ProcessSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const auto flushDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(this->flushIntervalMs);
    const size_t bufferSize = tupleBuffer.getBufferSize();
    size_t totalBytesInBuffer = 0;
    pollfd pfd{.fd = this->fd, .events = POLLIN, .revents = 0};

    while (not stopToken.stop_requested())
    {
        /// Waiting for data to be written to the pipe as long as the timeout was not reached
        const int pollResult = poll(&pfd, 1, static_cast<int>(this->pollTimeoutMs));
        if (pollResult < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            throw RunningRoutineFailure("poll() failed on pipe: {} - {}", this->pipePath, getErrorMessageFromERRNO());
        }
        if (pollResult == 0)
        {
            /// Timeout with no data. If buffer is not empty and flush deadline is reached, return buffer, otherwise, loop back to check stopToken
            if (totalBytesInBuffer > 0 && this->flushIntervalMs > 0 && std::chrono::steady_clock::now() >= flushDeadline)
            {
                this->totalNumBytesRead += totalBytesInBuffer;
                return FillTupleBufferResult::withBytes(totalBytesInBuffer);
            }
            continue;
        }
        if ((pfd.revents & (POLLERR | POLLNVAL)) != 0)
        {
            throw RunningRoutineFailure("poll() reported error on pipe: {}", this->pipePath);
        }

        /// Ready to read from the pipe
        if ((pfd.revents & (POLLIN | POLLHUP)) != 0)
        {
            const auto bytesRead
                = read(this->fd, tupleBuffer.getAvailableMemoryArea<char>().data() + totalBytesInBuffer, bufferSize - totalBytesInBuffer);
            if (bytesRead < 0)
            {
                throw RunningRoutineFailure("read() failed on pipe: {} - {}", this->pipePath, getErrorMessageFromERRNO());
            }
            if (bytesRead == 0)
            {
                /// Writer closed the pipe, return any accumulated bytes
                if (totalBytesInBuffer > 0)
                {
                    this->totalNumBytesRead += totalBytesInBuffer;
                    return FillTupleBufferResult::withBytes(totalBytesInBuffer);
                }
                return FillTupleBufferResult::eos();
            }
            totalBytesInBuffer += bytesRead;

            /// Without flush interval or if buffer is full, return immediately after first read
            if (this->flushIntervalMs == 0 || totalBytesInBuffer >= bufferSize)
            {
                this->totalNumBytesRead += totalBytesInBuffer;
                return FillTupleBufferResult::withBytes(totalBytesInBuffer);
            }

            /// With flush interval, check if deadline has passed
            if (std::chrono::steady_clock::now() >= flushDeadline)
            {
                this->totalNumBytesRead += totalBytesInBuffer;
                return FillTupleBufferResult::withBytes(totalBytesInBuffer);
            }
        }
    }

    /// Stop requested, return any accumulated bytes
    if (totalBytesInBuffer > 0)
    {
        this->totalNumBytesRead += totalBytesInBuffer;
        return FillTupleBufferResult::withBytes(totalBytesInBuffer);
    }
    return FillTupleBufferResult::eos();
}

DescriptorConfig::Config ProcessSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersProcess>(std::move(config), NAME);
}

std::ostream& ProcessSource::toString(std::ostream& str) const
{
    str << std::format(
        "\nProcessSource(pipePath: {}, pollTimeoutMs: {}, flushIntervalMs: {}, totalNumBytesRead: {})",
        this->pipePath,
        this->pollTimeoutMs,
        this->flushIntervalMs,
        this->totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType RegisterProcessSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return ProcessSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterProcessSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<ProcessSource>(sourceRegistryArguments.sourceDescriptor);
}

}
