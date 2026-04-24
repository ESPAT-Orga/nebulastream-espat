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
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// A source that reads an entire CSV file into memory during setup, parses every record into a native
/// row-layout TupleBuffer, and then replays these pre-formatted buffers during query execution. This
/// removes CSV parsing from the query hot path, which matters for in-memory benchmarks.
class MemorySource final : public Source
{
public:
    static constexpr std::string_view NAME = "Memory";

    explicit MemorySource(const SourceDescriptor& sourceDescriptor, size_t bufferSizeInBytes);
    ~MemorySource() override = default;

    MemorySource(const MemorySource&) = delete;
    MemorySource& operator=(const MemorySource&) = delete;
    MemorySource(MemorySource&&) = delete;
    MemorySource& operator=(MemorySource&&) = delete;


    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    bool setup(const std::shared_ptr<AbstractBufferProvider>& bufferProvider) override;

    void open(std::shared_ptr<AbstractBufferProvider>) override { }

    void close() override { }

    /// validates and formats a string to string configuration
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string filePath;
    Schema schema;
    ParserConfig parserConfig;
    size_t bufferSizeInBytes;
    std::atomic<size_t> totalTuplesEmitted{0};
    std::vector<TupleBuffer> preFormattedBuffers;
    std::vector<TupleBuffer>::iterator preFormattedBuffersIter;
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
