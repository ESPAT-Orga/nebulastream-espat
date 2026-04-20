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

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <grpcpp/server.h>

namespace NES
{

/// Config parameters for the gRPC source plugin.
struct ConfigParametersGrpcSource
{
    static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "grpc_port", 0, [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PORT, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> RECEIVE_TIMEOUT_MS{
        "receive_timeout_ms",
        1000,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(RECEIVE_TIMEOUT_MS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, PORT, RECEIVE_TIMEOUT_MS);
};

/// A request that has been received via gRPC and is waiting to be turned into a tuple.
struct PendingStatisticRequest
{
    uint64_t statisticId;
    uint64_t startTs;
    uint64_t endTs;
};

/// A source that runs a gRPC server implementing StatisticSourceService.
/// The coordinator calls RequestStatistic() to push requests into the source.
/// fillTupleBuffer() pulls from an internal queue and writes fields into the TupleBuffer.
class GrpcSource : public Source
{
public:
    static const std::string& name()
    {
        static const std::string Instance = "Grpc";
        return Instance;
    }

    explicit GrpcSource(const SourceDescriptor& sourceDescriptor);
    ~GrpcSource() override = default;

    GrpcSource(const GrpcSource&) = delete;
    GrpcSource& operator=(const GrpcSource&) = delete;
    GrpcSource(GrpcSource&&) = delete;
    GrpcSource& operator=(GrpcSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    /// Enqueues a request received via gRPC. Called by the service handler.
    void enqueueRequest(PendingStatisticRequest request);

    /// Returns the port the gRPC server is actually listening on (useful when configured with port 0).
    [[nodiscard]] uint32_t getActualPort() const { return actualPort; }

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    uint32_t configuredPort;
    uint32_t actualPort{0};
    uint32_t receiveTimeoutMs;
    std::shared_ptr<const Schema> schema;

    std::unique_ptr<grpc::Server> grpcServer;

    /// Thread-safe queue for incoming requests.
    std::mutex queueMutex;
    std::condition_variable queueCv;
    std::queue<PendingStatisticRequest> requestQueue;
};

}
