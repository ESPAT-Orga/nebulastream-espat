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
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>

#include <prometheus/exposer.h>
#include <prometheus/histogram.h>
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
    , histogramNumBuckets(sinkDescriptor.getFromConfig(ConfigParametersPrometheus::HISTOGRAM_NUM_BUCKETS))
    , histogramMinValue(sinkDescriptor.getFromConfig(ConfigParametersPrometheus::HISTOGRAM_MIN_VALUE))
    , histogramMaxValue(sinkDescriptor.getFromConfig(ConfigParametersPrometheus::HISTOGRAM_MAX_VALUE))
{
    INVARIANT(histogramNumBuckets > 0, "histogram_num_buckets must be > 0 (got {})", histogramNumBuckets);
    INVARIANT(
        histogramMaxValue > histogramMinValue,
        "histogram_max_value ({}) must be > histogram_min_value ({})",
        histogramMaxValue,
        histogramMinValue);
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

    /// Compute equi-width bucket boundaries once: numBuckets finite boundaries with the last equal to histogramMaxValue,
    /// so prometheus-cpp materializes numBuckets+1 buckets including the implicit +Inf overflow. Values > histogramMaxValue
    /// land in the overflow bucket; values < histogramMinValue land in the first bucket.
    prometheus::Histogram::BucketBoundaries boundaries;
    boundaries.reserve(histogramNumBuckets);
    const double width = (histogramMaxValue - histogramMinValue) / static_cast<double>(histogramNumBuckets);
    for (uint64_t i = 1; i <= histogramNumBuckets; ++i)
    {
        boundaries.push_back(histogramMinValue + static_cast<double>(i) * width);
    }

    size_t offset = 0;
    for (const auto& field : schema.getFields())
    {
        INVARIANT(field.dataType.isNumeric(), "Prometheus sink supports only numeric fields");

        /// replace '$' character which prometheus-cpp cannot handle by '_' which is used by convention as a delimiter in prometheus
        /// metric names
        std::string nameCopy = field.name;
        std::ranges::replace(nameCopy, '$', '_');

        auto& family = prometheus::BuildHistogram()
                           .Name(nameCopy)
                           .Help(fmt::format("NebulaStream sink histogram for field {}", nameCopy))
                           .Register(*registry);
        /// Cache the Histogram& once per field: per-tuple `Family::Add({}, ...)` would re-acquire the family's mutex
        /// for every observation. The per-tuple cost we actually want to measure is Histogram::Observe's internal
        /// mutex + bucket-find + counter increment.
        auto& histogram = family.Add({}, boundaries);
        metrics.push_back({&histogram, offset, field.dataType.type});
        offset += field.dataType.getSizeInBytesWithoutNull();
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
        for (auto [histogram, fieldOffset, type] : metrics)
        {
            /// Read the typed value out of the raw tuple bytes and widen to double; Histogram::Observe always takes a
            /// double. UINT64/INT64 values above 2^53 would lose precision on the widening cast, which is fine for the
            /// benchmark's [0, 1e6] and 1e9 sequence schemas.
            double value = 0.0;
            switch (type)
            {
                case DataType::Type::UINT8:
                    value = *reinterpret_cast<const uint8_t*>(&tuple[fieldOffset]);
                    break;
                case DataType::Type::UINT16:
                    value = *reinterpret_cast<const uint16_t*>(&tuple[fieldOffset]);
                    break;
                case DataType::Type::UINT32:
                    value = *reinterpret_cast<const uint32_t*>(&tuple[fieldOffset]);
                    break;
                case DataType::Type::UINT64:
                    value = static_cast<double>(*reinterpret_cast<const uint64_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::INT8:
                    value = *reinterpret_cast<const int8_t*>(&tuple[fieldOffset]);
                    break;
                case DataType::Type::INT16:
                    value = *reinterpret_cast<const int16_t*>(&tuple[fieldOffset]);
                    break;
                case DataType::Type::INT32:
                    value = *reinterpret_cast<const int32_t*>(&tuple[fieldOffset]);
                    break;
                case DataType::Type::INT64:
                    value = static_cast<double>(*reinterpret_cast<const int64_t*>(&tuple[fieldOffset]));
                    break;
                case DataType::Type::FLOAT32:
                    value = *reinterpret_cast<const float*>(&tuple[fieldOffset]);
                    break;
                case DataType::Type::FLOAT64:
                    value = *reinterpret_cast<const double*>(&tuple[fieldOffset]);
                    break;
                default:
                    INVARIANT(false, "Invalid field type in prometheus sink");
            }
            /// Histogram::Observe takes the histogram's internal std::mutex per call. The per-tuple cost is mutex +
            /// bucket-find + counter increment — this is the SOTA cost the paper measures.
            histogram->Observe(value);
        }
    }
}

void PrometheusSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Stopping prometheus sink, url={}", serverAddress);
}

DescriptorConfig::Config PrometheusSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    /// PrometheusSink reads typed values directly out of the input buffer via reinterpret_cast.
    /// Any non-NATIVE output_format inserts an OutputFormatterBufferRef (see PipeliningPhase)
    /// that converts records to formatted bytes (e.g. CSV ASCII) before the sink runs,
    /// which would cause the sink to read garbage. Reject anything but NATIVE up-front.
    if (const auto it = config.find(SinkDescriptor::OUTPUT_FORMAT.name); it != config.end())
    {
        if (NES::toUpperCase(it->second) != "NATIVE")
        {
            throw InvalidConfigParameter(
                "PrometheusSink requires output_format=NATIVE (got '{}'); any other format inserts an output "
                "formatter that converts records to bytes before they reach the sink.",
                it->second);
        }
    }
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
