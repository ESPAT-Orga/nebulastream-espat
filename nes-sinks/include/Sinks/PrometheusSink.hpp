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

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include <PipelineExecutionContext.hpp>

namespace NES
{
struct ExposedMetrics;

struct Metric
{
    prometheus::Family<prometheus::Gauge>* gauge;
    size_t fieldOffset;
    DataType::Type dataType;
};

/// A sink that writes formatted TupleBuffers to arbitrary files.
class PrometheusSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Prometheus";
    PrometheusSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~PrometheusSink() override = default;

    PrometheusSink(const PrometheusSink&) = delete;
    PrometheusSink& operator=(const PrometheusSink&) = delete;
    PrometheusSink(PrometheusSink&&) = delete;
    PrometheusSink& operator=(PrometheusSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    std::string serverAddress;
    prometheus::Exposer exposer;
    std::shared_ptr<prometheus::Registry> registry;
    std::vector<Metric> metrics;
    Schema schema;
};

struct ConfigParametersPrometheus
{
    static inline const DescriptorConfig::ConfigParameter<std::string> SERVER_URL{
        "server_url",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SERVER_URL, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            /// file path is kept here because systests expect it to be present
            SinkDescriptor::parameterMap,
            SinkDescriptor::FILE_PATH,
            SERVER_URL);
};
}
