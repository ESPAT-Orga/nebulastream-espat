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
#include <cstdint>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

class ProcessSource final : public Source
{
public:
    static constexpr std::string_view NAME = "Process";

    explicit ProcessSource(const SourceDescriptor& sourceDescriptor);
    ~ProcessSource() override = default;

    ProcessSource(const ProcessSource&) = delete;
    ProcessSource& operator=(const ProcessSource&) = delete;
    ProcessSource(ProcessSource&&) = delete;
    ProcessSource& operator=(ProcessSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open the named pipe for reading.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Close the named pipe.
    void close() override;

    /// Validates and formats a string-to-string configuration.
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    int fd = -1;
    std::string pipePath;
    uint32_t pollTimeoutMs;
    uint32_t flushIntervalMs;
    std::atomic<size_t> totalNumBytesRead;
};

struct ConfigParametersProcess
{
    static constexpr uint32_t DEFAULT_POLL_TIMEOUT_MS = 100;

    static inline const DescriptorConfig::ConfigParameter<std::string> PIPE_PATH{
        "pipe_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PIPE_PATH, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> POLL_TIMEOUT_MS{
        "poll_timeout_ms",
        DEFAULT_POLL_TIMEOUT_MS,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(POLL_TIMEOUT_MS, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> FLUSH_INTERVAL_MS{
        "flush_interval_ms",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, PIPE_PATH, POLL_TIMEOUT_MS, FLUSH_INTERVAL_MS);
};

}
