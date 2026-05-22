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
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
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
    /// One or two CSV files. When FILE_PATH_2 is set, the source alternates between the two
    /// after REPLAYS_PER_FILE full passes of the current one — used by the adaptive benchmark
    /// to simulate a workload-distribution shift on a deterministic schedule.
    std::vector<std::string> filePaths;
    bool loop;
    uint64_t replaysPerFile;
    Schema schema;
    ParserConfig parserConfig;
    size_t bufferSizeInBytes;
    std::atomic<size_t> totalTuplesEmitted{0};
    /// Outer vector: one entry per loaded file. Inner vector: pre-formatted row-layout buffers.
    std::vector<std::vector<TupleBuffer>> preFormattedBuffers;
    size_t currentFileIdx{0};
    uint64_t currentReplayCount{0};
    std::vector<TupleBuffer>::iterator currentBufferIter;
};

struct ConfigParametersCSVMemory
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    /// Optional second CSV. When set, the source alternates between the two files; switching
    /// from one to the other after REPLAYS_PER_FILE full passes of the current file.
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH_2{
        "file_path_2",
        std::string{},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH_2, config); }};

    /// How many full passes of the current file before advancing to the next one. Default 1 means
    /// "as soon as a file is fully drained, switch" — equivalent to seamless concatenation.
    static inline const DescriptorConfig::ConfigParameter<uint64_t> REPLAYS_PER_FILE{
        "replays_per_file",
        uint64_t{1},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(REPLAYS_PER_FILE, config); }};

    static inline const DescriptorConfig::ConfigParameter<bool> LOOP{
        "loop",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(LOOP, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH, FILEPATH_2, REPLAYS_PER_FILE, LOOP);
};

}
