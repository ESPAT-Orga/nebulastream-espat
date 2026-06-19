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

#include <MemorySource.hpp>

#include <cstddef>
#include <format>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <CsvBufferParser.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace
{
constexpr std::string_view FILE_PATH_PARAMETER = "file_path";
}

namespace NES
{

MemorySource::MemorySource(const SourceDescriptor& sourceDescriptor, const size_t bufferSizeInBytes)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSVMemory::FILEPATH))
    , schema(*sourceDescriptor.getLogicalSource().getSchema())
    , parserConfig(sourceDescriptor.getParserConfig())
    , bufferSizeInBytes(bufferSizeInBytes)
{
}

bool MemorySource::setup(const std::shared_ptr<AbstractBufferProvider>& bufferProvider)
{
    PRECONDITION(bufferProvider != nullptr, "Memory source setup requires a buffer provider");

    /// Parse the CSV file into native row-layout TupleBuffers ahead of time so CSV parsing stays
    /// out of the query hot path. The parse logic is shared with LoopingMemorySource.
    const auto layout = computeCsvRowLayout(schema, bufferSizeInBytes);
    parseCsvFileIntoBuffers(filePath, schema, layout, parserConfig, bufferSizeInBytes, *bufferProvider, preFormattedBuffers);

    preFormattedBuffersIter = preFormattedBuffers.begin();
    return true;
}

Source::FillTupleBufferResult MemorySource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    if (preFormattedBuffersIter == preFormattedBuffers.end())
    {
        return FillTupleBufferResult::eos();
    }

    /// Hand the pre-formatted buffer to the pipeline. A single pass moves each buffer out exactly
    /// once (zero-copy), releasing it as it is consumed.
    tupleBuffer = std::move(*preFormattedBuffersIter);
    const auto numTuples = tupleBuffer.getNumberOfTuples();
    totalTuplesEmitted += numTuples;
    ++preFormattedBuffersIter;
    return FillTupleBufferResult::withNativeTuples(numTuples);
}

DescriptorConfig::Config MemorySource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSVMemory>(std::move(config), NAME);
}

std::ostream& MemorySource::toString(std::ostream& str) const
{
    str << std::format("\nMemorySource(filepath: {}, totalTuplesEmitted: {})", this->filePath, this->totalTuplesEmitted.load());
    return str;
}

SourceValidationRegistryReturnType RegisterMemorySourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return MemorySource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMemorySource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<MemorySource>(sourceRegistryArguments.sourceDescriptor, sourceRegistryArguments.bufferSizeInBytes);
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterMemoryFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string(FILE_PATH_PARAMETER)))
    {
        throw InvalidConfigParameter("The mock memory data source cannot be used if the file_path parameter is already set.");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        std::string(FILE_PATH_PARAMETER), systestAdaptorArguments.testFilePath.string());

    return systestAdaptorArguments.physicalSourceConfig;
}

}
