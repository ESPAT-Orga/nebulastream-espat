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
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <PipelineExecutionContext.hpp>

namespace NES
{
struct ExposedMetrics;

struct Metric
{
    prometheus::Histogram* histogram;
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
    /// Equi-width bucket boundaries (numBuckets finite edges), computed once in start() and reused
    /// per buffer in execute() to bucket values for the batched ObserveMultiple flush.
    prometheus::Histogram::BucketBoundaries boundaries;
    Schema schema;
    uint64_t histogramNumBuckets;
    double histogramMinValue;
    double histogramMaxValue;
};

struct ConfigParametersPrometheus
{
    static inline const DescriptorConfig::ConfigParameter<std::string> SERVER_URL{
        "server_url",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SERVER_URL, config); }};

    /// file_path is parsed but unused; the legacy systest harness expects every sink config to carry it.
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        std::string{""},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    /// Equi-width histogram parameters. Defaults match the in-engine EQUIWIDTHHISTOGRAM operator's typical config so an SOTA
    /// Prometheus-backed deployment produces the same bucket layout as the in-engine operator for apples-to-apples comparison.
    static inline const DescriptorConfig::ConfigParameter<uint64_t> HISTOGRAM_NUM_BUCKETS{
        "histogram_num_buckets",
        100,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HISTOGRAM_NUM_BUCKETS, config); }};

    static inline const DescriptorConfig::ConfigParameter<double> HISTOGRAM_MIN_VALUE{
        "histogram_min_value",
        0.0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HISTOGRAM_MIN_VALUE, config); }};

    static inline const DescriptorConfig::ConfigParameter<double> HISTOGRAM_MAX_VALUE{
        "histogram_max_value",
        1000000.0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HISTOGRAM_MAX_VALUE, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap, FILE_PATH, SERVER_URL, HISTOGRAM_NUM_BUCKETS, HISTOGRAM_MIN_VALUE, HISTOGRAM_MAX_VALUE);
};
}
