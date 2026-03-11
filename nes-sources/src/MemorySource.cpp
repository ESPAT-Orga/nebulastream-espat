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

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <format>
#include <fstream>
#include <ios>
#include <iostream>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Files.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace
{
static constexpr std::string_view FILE_PATH_PARAMETER = "file_path";
}

namespace NES
{

MemorySource::MemorySource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSVMemory::FILEPATH))
{
}

bool MemorySource::setup()
{
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(filePath.c_str(), nullptr), std::free};
    if (not realCSVPath)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", filePath.c_str(), getErrorMessageFromERRNO());
    }

    const auto fileSize = std::filesystem::file_size(realCSVPath.get());
    fileData.resize(fileSize);

    auto inputFile = std::ifstream(realCSVPath.get(), std::ios::binary);
    if (not inputFile)
    {
        throw InvalidConfigParameter("Could not open file: {} - {}", filePath.c_str(), getErrorMessageFromERRNO());
    }

    inputFile.read(fileData.data(), static_cast<std::streamsize>(fileSize));

    const auto bytesRead = inputFile.gcount();
    if (static_cast<size_t>(bytesRead) != fileSize)
    {
        throw InvalidConfigParameter("Could not read entire file: {} (read {} of {} bytes)", filePath, bytesRead, fileSize);
    }

    std::cout << std::format("MemorySource: Loaded {} bytes from {}\n", fileSize, filePath);

    setupFinished = true;
    return true;
}

Source::FillTupleBufferResult MemorySource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    while (not setupFinished)
    {
    }

    if (currentOffset >= fileData.size())
    {
        return FillTupleBufferResult::eos();
    }

    const auto bytesToCopy = std::min(tupleBuffer.getBufferSize(), fileData.size() - currentOffset);
    std::memcpy(tupleBuffer.getAvailableMemoryArea<char>().data(), fileData.data() + currentOffset, bytesToCopy);
    currentOffset += bytesToCopy;
    totalNumBytesRead += bytesToCopy;

    return FillTupleBufferResult::withBytes(bytesToCopy);
}

DescriptorConfig::Config MemorySource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSVMemory>(std::move(config), NAME);
}

std::ostream& MemorySource::toString(std::ostream& str) const
{
    str << std::format("\nMemorySource(filepath: {}, totalNumBytesRead: {})", this->filePath, this->totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType RegisterMemorySourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return MemorySource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMemorySource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<MemorySource>(sourceRegistryArguments.sourceDescriptor);
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
