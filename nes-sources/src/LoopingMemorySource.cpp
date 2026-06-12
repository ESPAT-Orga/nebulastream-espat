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

#include <LoopingMemorySource.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <CsvBufferParser.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

LoopingMemorySource::LoopingMemorySource(const SourceDescriptor& sourceDescriptor, const size_t bufferSizeInBytes)
    : loop(sourceDescriptor.getFromConfig(ConfigParametersLoopingMemory::LOOP))
    , replaysPerFile(sourceDescriptor.getFromConfig(ConfigParametersLoopingMemory::REPLAYS_PER_FILE))
    , millisPerFile(sourceDescriptor.getFromConfig(ConfigParametersLoopingMemory::MILLIS_PER_FILE))
    , monotonicTimestampField(sourceDescriptor.getFromConfig(ConfigParametersLoopingMemory::MONOTONIC_TIMESTAMP_FIELD))
    , schema(*sourceDescriptor.getLogicalSource().getSchema())
    , parserConfig(sourceDescriptor.getParserConfig())
    , bufferSizeInBytes(bufferSizeInBytes)
{
    filePaths.push_back(sourceDescriptor.getFromConfig(ConfigParametersLoopingMemory::FILEPATH));
    const auto secondPath = sourceDescriptor.getFromConfig(ConfigParametersLoopingMemory::FILEPATH_2);
    if (not secondPath.empty())
    {
        filePaths.push_back(secondPath);
    }
}

bool LoopingMemorySource::setup(const std::shared_ptr<AbstractBufferProvider>& bufferProvider)
{
    PRECONDITION(bufferProvider != nullptr, "Memory source setup requires a buffer provider");

    /// Parse each CSV file into native row-layout TupleBuffers. The layout (offsets + tuple size)
    /// is computed once and reused for the per-replay timestamp patch below. Parsing is shared with
    /// MemorySource via CsvBufferParser.
    const auto layout = computeCsvRowLayout(schema, bufferSizeInBytes);

    preFormattedBuffers.reserve(filePaths.size());
    for (const auto& filePath : filePaths)
    {
        preFormattedBuffers.emplace_back();
        parseCsvFileIntoBuffers(filePath, schema, layout, parserConfig, bufferSizeInBytes, *bufferProvider, preFormattedBuffers.back());
    }

    if (preFormattedBuffers.empty() or preFormattedBuffers.front().empty())
    {
        throw InvalidConfigParameter("Memory source produced no buffers from {} file(s)", filePaths.size());
    }

    /// Cache tuple count per file (used to step globalTimestampOffset by exactly one cycle's worth).
    tuplesPerFile.reserve(preFormattedBuffers.size());
    for (const auto& fileBuffers : preFormattedBuffers)
    {
        uint64_t total = 0;
        for (const auto& buf : fileBuffers)
        {
            total += buf.getNumberOfTuples();
        }
        tuplesPerFile.push_back(total);
    }

    /// Resolve the monotonic timestamp field (suffix match, case-insensitive).
    monotonicTimestampOffsetBytes = -1;
    if (not monotonicTimestampField.empty())
    {
        const auto& fields = schema.getFields();
        const auto needle = toUpperCase(monotonicTimestampField);
        for (size_t i = 0; i < fields.size(); ++i)
        {
            if (toUpperCase(fields[i].name).ends_with(needle))
            {
                if (fields[i].dataType.type != DataType::Type::UINT64)
                {
                    throw InvalidConfigParameter(
                        "monotonic_timestamp_field {} must resolve to a UINT64 column, but field {} is not UINT64",
                        monotonicTimestampField,
                        fields[i].name);
                }
                monotonicTimestampOffsetBytes = static_cast<int>(layout.fieldByteOffsets[i]);
                break;
            }
        }
        if (monotonicTimestampOffsetBytes < 0)
        {
            throw InvalidConfigParameter(
                "monotonic_timestamp_field '{}' did not match (suffix, case-insensitive) any field in the schema", monotonicTimestampField);
        }
    }

    /// Cache for fillTupleBuffer.
    tupleSizeBytes = layout.tupleSize;
    this->bufferProvider = bufferProvider;

    currentFileIdx = 0;
    currentReplayCount = 0;
    globalTimestampOffset = 0;
    currentBufferIter = preFormattedBuffers[0].begin();
    return true;
}

Source::FillTupleBufferResult LoopingMemorySource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    /// Start the wall-clock regime timer on the first emit rather than in setup(), so the (possibly
    /// multi-second) CSV parse done in setup() does not eat into the first file's time budget.
    if (not fileTimerStarted)
    {
        currentFileStart = std::chrono::steady_clock::now();
        fileTimerStarted = true;
    }

    if (currentBufferIter == preFormattedBuffers[currentFileIdx].end())
    {
        /// Current file fully drained once. Bump the timestamp offset by one cycle's worth so
        /// downstream watermarks keep advancing across loops. (No-op when the feature is off.)
        globalTimestampOffset += tuplesPerFile[currentFileIdx];

        ++currentReplayCount;
        /// Decide whether to advance to the next file. Wall-clock mode (millisPerFile > 0) switches
        /// once this many milliseconds have elapsed on the current file, keeping every regime the
        /// same wall-clock duration regardless of throughput; otherwise switch after replaysPerFile
        /// full passes. The check runs at pass boundaries, so the actual switch lands at the end of
        /// the pass in flight when the budget is reached.
        const bool switchFile = millisPerFile > 0
            ? std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - currentFileStart).count()
                >= static_cast<int64_t>(millisPerFile)
            : currentReplayCount >= replaysPerFile;
        if (switchFile)
        {
            ++currentFileIdx;
            currentReplayCount = 0;
            if (currentFileIdx >= preFormattedBuffers.size())
            {
                if (not loop)
                {
                    return FillTupleBufferResult::eos();
                }
                currentFileIdx = 0;
            }
            currentFileStart = std::chrono::steady_clock::now();
        }
        if (preFormattedBuffers[currentFileIdx].empty())
        {
            return FillTupleBufferResult::eos();
        }
        currentBufferIter = preFormattedBuffers[currentFileIdx].begin();
    }

    if (monotonicTimestampOffsetBytes < 0 or globalTimestampOffset == 0)
    {
        /// Fast path: feature disabled or first cycle of first file. Share the pre-formatted
        /// buffer directly (refcount bump).
        tupleBuffer = *currentBufferIter;
    }
    else
    {
        /// Allocate a fresh buffer, copy from the pre-formatted source, then patch the timestamp
        /// column with the running offset. This is only walked on replay cycles ≥ 1.
        const auto& sourceBuffer = *currentBufferIter;
        auto unpooled = bufferProvider->getUnpooledBuffer(bufferSizeInBytes);
        if (not unpooled.has_value())
        {
            return FillTupleBufferResult::eos();
        }
        TupleBuffer fresh = std::move(unpooled.value());
        const auto numTuples = sourceBuffer.getNumberOfTuples();
        const auto bytesToCopy = numTuples * tupleSizeBytes;
        std::memcpy(fresh.getAvailableMemoryArea<char>().data(), sourceBuffer.getAvailableMemoryArea<char>().data(), bytesToCopy);
        char* const base = fresh.getAvailableMemoryArea<char>().data();
        for (uint64_t i = 0; i < numTuples; ++i)
        {
            char* const slot = base + (i * tupleSizeBytes) + static_cast<size_t>(monotonicTimestampOffsetBytes);
            uint64_t ts;
            std::memcpy(&ts, slot, sizeof(uint64_t));
            ts += globalTimestampOffset;
            std::memcpy(slot, &ts, sizeof(uint64_t));
        }
        fresh.setNumberOfTuples(numTuples);
        tupleBuffer = std::move(fresh);
    }

    const auto numTuples = tupleBuffer.getNumberOfTuples();
    totalTuplesEmitted += numTuples;
    ++currentBufferIter;
    return FillTupleBufferResult::withNativeTuples(numTuples);
}

DescriptorConfig::Config LoopingMemorySource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersLoopingMemory>(std::move(config), NAME);
}

std::ostream& LoopingMemorySource::toString(std::ostream& str) const
{
    str << "\nLoopingMemorySource(filepaths:";
    for (const auto& path : this->filePaths)
    {
        str << " " << path;
    }
    str << std::format(
        ", replaysPerFile: {}, millisPerFile: {}, totalTuplesEmitted: {})",
        this->replaysPerFile,
        this->millisPerFile,
        this->totalTuplesEmitted.load());
    return str;
}

SourceValidationRegistryReturnType RegisterLoopingMemorySourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return LoopingMemorySource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterLoopingMemorySource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<LoopingMemorySource>(sourceRegistryArguments.sourceDescriptor, sourceRegistryArguments.bufferSizeInBytes);
}

}
