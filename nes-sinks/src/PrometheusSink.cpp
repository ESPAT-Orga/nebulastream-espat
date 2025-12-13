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

#include <Sinks/PrometheusSink.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include <sys/socket.h>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

PrometheusSink::PrometheusSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , serverAddress(sinkDescriptor.getFromConfig(ConfigParametersPrometheus::SERVER_URL))
    , exposer(sinkDescriptor.getFromConfig(ConfigParametersPrometheus::SERVER_URL))
    , registry(std::make_shared<prometheus::Registry>())
    , schema(*sinkDescriptor.getSchema())
{
    NES_DEBUG("prometheus exposer running on {}", serverAddress);
}

std::ostream& PrometheusSink::toString(std::ostream& str) const
{
    str << fmt::format("PrometheusSink(url: {})", serverAddress);
    return str;
}

void PrometheusSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("register prometheus metrics");
    size_t offset = 0;
    for (const auto& field : schema.getFields())
    {
        INVARIANT(field.dataType.isNumeric(), "Prometheus sink supports only numeric fields");

        /// replace '$' character because prometheus-cpp cannot handle it
        std::string nameCopy = field.name;
        std::ranges::replace(nameCopy, '$', '_');

        auto gauge = &(prometheus::BuildGauge().Name(nameCopy).Help("Derived from nes prometheus sink schema").Register(*registry));
        metrics.push_back({gauge, offset, field.dataType.type});
        offset += field.dataType.getSizeInBytes();
    }

    /// ask the exposer to scrape the registry on incoming HTTP requests
    exposer.RegisterCollectable(registry);
}

void PrometheusSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in FileSink.");
    NES_DEBUG("Executing prometheus sink");

    const auto numberOfTuples = inputTupleBuffer.getNumberOfTuples();
    for (size_t i = 0; i < numberOfTuples; i++)
    {
        const auto* tuple = &inputTupleBuffer.getAvailableMemoryArea<>()[i * schema.getSizeOfSchemaInBytes()];
        for (auto [gauge, fieldOffset, type] : metrics)
        {
            switch (type)
            {
                case DataType::Type::UINT8:
                    gauge->Add({}).Set(*reinterpret_cast<const uint8_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::UINT16:
                    gauge->Add({}).Set(*reinterpret_cast<const uint16_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::UINT32:
                    gauge->Add({}).Set(*reinterpret_cast<const uint32_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::UINT64:
                    gauge->Add({}).Set(*reinterpret_cast<const uint64_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::INT8:
                    gauge->Add({}).Set(*reinterpret_cast<const int8_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::INT16:
                    gauge->Add({}).Set(*reinterpret_cast<const int16_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::INT32:
                    gauge->Add({}).Set(*reinterpret_cast<const int32_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::INT64:
                    gauge->Add({}).Set(*reinterpret_cast<const int64_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::FLOAT32:
                    gauge->Add({}).Set(*reinterpret_cast<const float*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::FLOAT64:
                    gauge->Add({}).Set(*reinterpret_cast<const double*>(&tuple[fieldOffset]));
                    break;
                default:
                    INVARIANT(false, "Invalid field type in prometheus sink");
            }
        }
    }
}

void PrometheusSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Stopping prometheus sink, url={}", serverAddress);
}

DescriptorConfig::Config PrometheusSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersPrometheus>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterPrometheusSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return PrometheusSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterPrometheusSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<PrometheusSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
