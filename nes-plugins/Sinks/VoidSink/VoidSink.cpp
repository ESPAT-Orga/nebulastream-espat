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
#include <VoidSink.hpp>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{
VoidSink::VoidSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , outputFilePath(sinkDescriptor.getFromConfig(SinkDescriptor::FILE_PATH))
    , schema(sinkDescriptor.getSchema())

{
}

void VoidSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up void sink: {}", *this);

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
    ss << schema->getFields().front().name << ":" << magic_enum::enum_name(schema->getFields().front().dataType.type) << ":"
       << magic_enum::enum_name(
              schema->getFields().front().dataType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    for (const auto& field : schema->getFields() | std::views::drop(1))
    {
        ss << ',' << field.name << ':' << magic_enum::enum_name(field.dataType.type) << ":"
           << magic_enum::enum_name(field.dataType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    }
    outputFileStream << ss.str() << '\n';
    outputFileStream.close();
}

void VoidSink::stop(PipelineExecutionContext&)
{
    NES_INFO("Void Sink completed.")
}

void VoidSink::execute([[maybe_unused]] const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in VoidSink.");
}

DescriptorConfig::Config VoidSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersVoid>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterVoidSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return VoidSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterVoidSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<VoidSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
