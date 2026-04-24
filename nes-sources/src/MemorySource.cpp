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
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <format>
#include <fstream>
#include <ios>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Files.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace
{
static constexpr std::string_view FILE_PATH_PARAMETER = "file_path";

/// Write a fixed-size value parsed from a CSV field into the row-layout slot. Throws on malformed input.
template <typename T>
void writeFixedValue(char* slot, std::string_view field)
{
    const T value = NES::from_chars_with_exception<T>(NES::trimWhiteSpaces(field));
    std::memcpy(slot, &value, sizeof(T));
}

void writeScalar(char* slot, NES::DataType::Type type, std::string_view field)
{
    using T = NES::DataType::Type;
    switch (type)
    {
        case T::UINT8:
            writeFixedValue<std::uint8_t>(slot, field);
            return;
        case T::UINT16:
            writeFixedValue<std::uint16_t>(slot, field);
            return;
        case T::UINT32:
            writeFixedValue<std::uint32_t>(slot, field);
            return;
        case T::UINT64:
            writeFixedValue<std::uint64_t>(slot, field);
            return;
        case T::INT8:
            writeFixedValue<std::int8_t>(slot, field);
            return;
        case T::INT16:
            writeFixedValue<std::int16_t>(slot, field);
            return;
        case T::INT32:
            writeFixedValue<std::int32_t>(slot, field);
            return;
        case T::INT64:
            writeFixedValue<std::int64_t>(slot, field);
            return;
        case T::FLOAT32:
            writeFixedValue<float>(slot, field);
            return;
        case T::FLOAT64:
            writeFixedValue<double>(slot, field);
            return;
        case T::BOOLEAN:
            writeFixedValue<bool>(slot, field);
            return;
        case T::CHAR:
            writeFixedValue<char>(slot, field);
            return;
        case T::VARSIZED:
        case T::UNDEFINED:
            throw NES::CannotFormatSourceData("writeScalar called on non-scalar type");
    }
}

/// Append varSized bytes into a child buffer (reusing the most recent child if there is room, otherwise
/// allocating a new one). Mirrors the policy in TupleBufferRef::writeVarSized but avoids the Nautilus
/// dependency so nes-sources doesn't need to link nes-nautilus. We allocate child buffers as unpooled
/// so parsing the whole file during setup() never competes with the source's bounded local pool.
NES::VariableSizedAccess
appendVarSized(NES::TupleBuffer& parent, NES::AbstractBufferProvider& bufferProvider, const std::string_view varSizedValue)
{
    const auto totalVarSizedLength = varSizedValue.size();
    const auto emplaceIntoFreshChild = [&]()
    {
        const size_t requiredBufferSize = std::max<size_t>(bufferProvider.getBufferSize(), totalVarSizedLength);
        auto unpooled = bufferProvider.getUnpooledBuffer(requiredBufferSize);
        if (not unpooled.has_value())
        {
            throw NES::CannotAllocateBuffer("Cannot allocate child buffer of size {}", requiredBufferSize);
        }
        auto newChildBuffer = std::move(unpooled.value());
        if (totalVarSizedLength > 0)
        {
            std::memcpy(newChildBuffer.getAvailableMemoryArea<char>().data(), varSizedValue.data(), totalVarSizedLength);
        }
        newChildBuffer.setNumberOfTuples(totalVarSizedLength);
        const auto childBufferIndex = parent.storeChildBuffer(newChildBuffer);
        return NES::VariableSizedAccess{childBufferIndex, NES::VariableSizedAccess::Size{totalVarSizedLength}};
    };

    const auto numberOfChildBuffers = parent.getNumberOfChildBuffers();
    if (numberOfChildBuffers == 0)
    {
        return emplaceIntoFreshChild();
    }

    const NES::VariableSizedAccess::Index childIndex{numberOfChildBuffers - 1};
    auto lastChildBuffer = parent.loadChildBuffer(childIndex);
    const auto usedMemorySize = lastChildBuffer.getNumberOfTuples();
    if (usedMemorySize + totalVarSizedLength >= lastChildBuffer.getBufferSize())
    {
        return emplaceIntoFreshChild();
    }

    const NES::VariableSizedAccess::Offset childOffset{usedMemorySize};
    if (totalVarSizedLength > 0)
    {
        std::memcpy(lastChildBuffer.getAvailableMemoryArea<char>().data() + usedMemorySize, varSizedValue.data(), totalVarSizedLength);
    }
    lastChildBuffer.setNumberOfTuples(usedMemorySize + totalVarSizedLength);
    return NES::VariableSizedAccess{childIndex, childOffset, NES::VariableSizedAccess::Size{totalVarSizedLength}};
}

/// Splits a single-character tuple into its fields. CSV double-quote handling mirrors CSVInputFormatIndexer's
/// 'commas in strings' path only when the parser was configured that way.
void splitFields(std::string_view tuple, char fieldDelimiter, bool allowCommasInStrings, std::vector<std::string_view>& outFields)
{
    outFields.clear();
    if (allowCommasInStrings)
    {
        size_t start = 0;
        bool isQuoted = false;
        for (size_t i = 0; i < tuple.size(); ++i)
        {
            const char c = tuple[i];
            if (not isQuoted and c == fieldDelimiter)
            {
                outFields.emplace_back(tuple.substr(start, i - start));
                start = i + 1;
            }
            if (c == '"')
            {
                isQuoted = not isQuoted;
            }
        }
        outFields.emplace_back(tuple.substr(start));
        return;
    }
    size_t start = 0;
    for (size_t i = tuple.find(fieldDelimiter); i != std::string_view::npos; i = tuple.find(fieldDelimiter, start))
    {
        outFields.emplace_back(tuple.substr(start, i - start));
        start = i + 1;
    }
    outFields.emplace_back(tuple.substr(start));
}
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

    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(filePath.c_str(), nullptr), std::free};
    if (not realCSVPath)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", filePath.c_str(), getErrorMessageFromERRNO());
    }

    const auto fileSize = std::filesystem::file_size(realCSVPath.get());
    std::vector<char> fileData(fileSize);
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

    /// Parse CSV -> row-layout TupleBuffers.
    INVARIANT(parserConfig.tupleDelimiter.size() == 1, "MemorySource only supports single-character tuple delimiters");
    INVARIANT(parserConfig.fieldDelimiter.size() == 1, "MemorySource only supports single-character field delimiters");
    const char tupleDelimiter = parserConfig.tupleDelimiter.front();
    const char fieldDelimiter = parserConfig.fieldDelimiter.front();

    const auto& fields = schema.getFields();
    const size_t numberOfFields = fields.size();
    const size_t tupleSize = schema.getSizeOfSchemaInBytes();
    if (tupleSize == 0 or numberOfFields == 0)
    {
        throw InvalidConfigParameter("Memory source requires a non-empty schema");
    }
    if (tupleSize > bufferSizeInBytes)
    {
        throw TuplesTooLargeForPipelineBufferSize(
            "Tuple size of {} bytes exceeds configured buffer size of {} bytes", tupleSize, bufferSizeInBytes);
    }
    const size_t tuplesPerBuffer = bufferSizeInBytes / tupleSize;

    std::vector<size_t> fieldByteOffsets(numberOfFields, 0);
    for (size_t i = 1; i < numberOfFields; ++i)
    {
        fieldByteOffsets[i] = fieldByteOffsets[i - 1] + fields[i - 1].dataType.getSizeInBytesWithoutNull();
    }

    const std::string_view fileView{fileData.data(), fileData.size()};
    std::vector<std::string_view> fieldViews;
    fieldViews.reserve(numberOfFields);

    std::optional<TupleBuffer> currentBuffer;
    size_t currentTupleIdx = 0;
    size_t totalTuples = 0;

    const auto finalizeCurrent = [&]()
    {
        if (currentBuffer.has_value())
        {
            currentBuffer->setNumberOfTuples(currentTupleIdx);
            preFormattedBuffers.emplace_back(std::move(currentBuffer.value()));
            currentBuffer.reset();
        }
        currentTupleIdx = 0;
    };

    size_t tupleStart = 0;
    for (size_t delimiterIdx = fileView.find(tupleDelimiter); delimiterIdx != std::string_view::npos;
         delimiterIdx = fileView.find(tupleDelimiter, tupleStart))
    {
        const auto tuple = fileView.substr(tupleStart, delimiterIdx - tupleStart);
        tupleStart = delimiterIdx + 1;
        if (tuple.empty())
        {
            continue;
        }

        splitFields(tuple, fieldDelimiter, parserConfig.allowCommasInStrings, fieldViews);
        if (fieldViews.size() != numberOfFields)
        {
            throw CannotFormatSourceData(
                "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema)",
                fieldViews.size(),
                numberOfFields);
        }

        if (not currentBuffer.has_value())
        {
            auto unpooled = bufferProvider->getUnpooledBuffer(bufferSizeInBytes);
            if (not unpooled.has_value())
            {
                throw CannotAllocateBuffer("Cannot allocate pre-formatted buffer of size {}", bufferSizeInBytes);
            }
            currentBuffer = std::move(unpooled.value());
            std::memset(currentBuffer->getAvailableMemoryArea<char>().data(), 0, currentBuffer->getBufferSize());
        }

        char* const rowBase = currentBuffer->getAvailableMemoryArea<char>().data() + (currentTupleIdx * tupleSize);
        for (size_t i = 0; i < numberOfFields; ++i)
        {
            char* const slot = rowBase + fieldByteOffsets[i];
            const auto& dataType = fields[i].dataType;
            if (dataType.type == DataType::Type::VARSIZED)
            {
                const auto access = appendVarSized(*currentBuffer, *bufferProvider, fieldViews[i]);
                std::memcpy(slot, &access, sizeof(VariableSizedAccess));
            }
            else
            {
                writeScalar(slot, dataType.type, fieldViews[i]);
            }
        }

        ++currentTupleIdx;
        ++totalTuples;
        if (currentTupleIdx >= tuplesPerBuffer)
        {
            finalizeCurrent();
        }
    }
    /// Trailing line without a tuple delimiter: parse it as a final record.
    if (tupleStart < fileView.size())
    {
        const auto tuple = fileView.substr(tupleStart);
        if (not tuple.empty())
        {
            splitFields(tuple, fieldDelimiter, parserConfig.allowCommasInStrings, fieldViews);
            if (fieldViews.size() == numberOfFields)
            {
                if (not currentBuffer.has_value())
                {
                    currentBuffer = bufferProvider->getBufferBlocking();
                    std::memset(currentBuffer->getAvailableMemoryArea<char>().data(), 0, currentBuffer->getBufferSize());
                }
                char* const rowBase = currentBuffer->getAvailableMemoryArea<char>().data() + (currentTupleIdx * tupleSize);
                for (size_t i = 0; i < numberOfFields; ++i)
                {
                    char* const slot = rowBase + fieldByteOffsets[i];
                    const auto& dataType = fields[i].dataType;
                    if (dataType.type == DataType::Type::VARSIZED)
                    {
                        const auto access = appendVarSized(*currentBuffer, *bufferProvider, fieldViews[i]);
                        std::memcpy(slot, &access, sizeof(VariableSizedAccess));
                    }
                    else
                    {
                        writeScalar(slot, dataType.type, fieldViews[i]);
                    }
                }
                ++currentTupleIdx;
                ++totalTuples;
            }
        }
    }
    finalizeCurrent();

    std::cout << std::format(
        "MemorySource: Loaded {} bytes from {}, pre-formatted {} tuples across {} buffers\n",
        fileSize,
        filePath,
        totalTuples,
        preFormattedBuffers.size());

    preFormattedBuffersIter = preFormattedBuffers.begin();
    setupFinished = true;
    return true;
}

Source::FillTupleBufferResult MemorySource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    while (not setupFinished) [[unlikely]]
    {
        std::cout << "Waiting on setup finishing!" << std::endl;
    }

    if (preFormattedBuffersIter == preFormattedBuffers.end())
    {
        return FillTupleBufferResult::eos();
    }

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
