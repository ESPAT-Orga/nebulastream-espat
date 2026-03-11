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
#include <DiscardSink.hpp>

#include <filesystem>
#include <fstream>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

DiscardSink::DiscardSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController)), outputFilePath(sinkDescriptor.getFromConfig(SinkDescriptor::FILE_PATH))
    , schema(sinkDescriptor.getSchema())
{
}

void DiscardSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up discard sink: {}", *this);

    if (std::filesystem::exists(outputFilePath))
    {
        std::error_code ec;
        if (!std::filesystem::remove(outputFilePath, ec))
        {
            throw CannotOpenSink("Could not remove existing output file: filePath={} ", outputFilePath);
        }
    }

    /// Write the schema header so the result checker can parse the file
    std::ofstream outputFileStream{outputFilePath, std::ofstream::binary};
    if (!outputFileStream.is_open() || !outputFileStream.good())
    {
        throw CannotOpenSink("Could not open output file: filePath={}", outputFilePath);
    }

    std::stringstream ss;
    ss << schema->getFields().front().name << ":" << magic_enum::enum_name(schema->getFields().front().dataType.type);
    for (const auto& field : schema->getFields() | std::views::drop(1))
    {
        ss << ',' << field.name << ':' << magic_enum::enum_name(field.dataType.type);
    }
    outputFileStream << ss.str() << '\n';
    outputFileStream.close();
}

void DiscardSink::stop(PipelineExecutionContext&)
{
    NES_INFO("Discard Sink completed.")
}

void DiscardSink::execute([[maybe_unused]] const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    /// This sink discards the tuple buffer
}

DescriptorConfig::Config DiscardSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersDiscard>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterDiscardSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return DiscardSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterDiscardSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<DiscardSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
