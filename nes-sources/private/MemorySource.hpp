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
#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// A source that reads an entire CSV file into memory during setup, then replays the raw data
/// chunk by chunk during query execution. This eliminates disk I/O overhead during benchmarks.
class MemorySource final : public Source
{
public:
    static constexpr std::string_view NAME = "Memory";

    explicit MemorySource(const SourceDescriptor& sourceDescriptor);
    ~MemorySource() override = default;

    MemorySource(const MemorySource&) = delete;
    MemorySource& operator=(const MemorySource&) = delete;
    MemorySource(MemorySource&&) = delete;
    MemorySource& operator=(MemorySource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    bool setup() override;

    void open(std::shared_ptr<AbstractBufferProvider>) override { }

    void close() override { }

    /// validates and formats a string to string configuration
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string filePath;
    std::atomic<size_t> totalNumBytesRead{0};
    std::vector<char> fileData;
    size_t currentOffset{0};
    std::atomic<bool> setupFinished{false};
};

struct ConfigParametersCSVMemory
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
