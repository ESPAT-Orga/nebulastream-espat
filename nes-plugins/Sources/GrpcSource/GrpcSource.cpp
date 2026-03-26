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

#include <GrpcSource.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/status.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>

namespace NES
{

/// gRPC service handler that forwards incoming requests to the GrpcSource queue.
class StatisticSourceServiceImpl final : public StatisticSourceService::Service
{
public:
    explicit StatisticSourceServiceImpl(GrpcSource& source) : source(source) { }

    grpc::Status RequestStatistic(grpc::ServerContext*, const StatisticRequest* request, google::protobuf::Empty*) override
    {
        source.enqueueRequest(PendingStatisticRequest{
            .statisticId = request->statistic_id(),
            .startTs = request->start_ts(),
            .endTs = request->end_ts(),
        });
        return grpc::Status::OK;
    }

private:
    GrpcSource& source;
};

GrpcSource::GrpcSource(const SourceDescriptor& sourceDescriptor)
    : configuredPort(sourceDescriptor.getFromConfig(ConfigParametersGrpcSource::PORT))
    , receiveTimeoutMs(sourceDescriptor.getFromConfig(ConfigParametersGrpcSource::RECEIVE_TIMEOUT_MS))
    , schema(sourceDescriptor.getLogicalSource().getSchema())
{
    NES_TRACE("Init GrpcSource on port {}.", configuredPort);
}

void GrpcSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_DEBUG("GrpcSource::open: Starting gRPC server.");

    auto service = std::make_unique<StatisticSourceServiceImpl>(*this);
    grpc::ServerBuilder builder;
    int selectedPort = 0;
    builder.AddListeningPort("0.0.0.0:" + std::to_string(configuredPort), grpc::InsecureServerCredentials(), &selectedPort);
    builder.RegisterService(service.get());
    grpcServer = builder.BuildAndStart();
    if (not grpcServer)
    {
        throw CannotOpenSource("GrpcSource: Failed to start gRPC server on port {}", configuredPort);
    }
    actualPort = static_cast<uint32_t>(selectedPort);
    NES_INFO("GrpcSource: gRPC server listening on port {}.", actualPort);
    /// Transfer ownership of the service to the server by releasing it here.
    /// The server will keep it alive as long as it is running.
    service.release(); /// NOLINT(bugprone-unused-return-value)
}

Source::FillTupleBufferResult GrpcSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    std::unique_lock lock{queueMutex};
    const auto timeout = std::chrono::milliseconds{receiveTimeoutMs};

    /// Wait until data is available, the stop token fires, or the timeout expires.
    queueCv.wait_for(lock, timeout, [&] { return !requestQueue.empty() || stopToken.stop_requested(); });

    if (requestQueue.empty())
    {
        if (stopToken.stop_requested())
        {
            return FillTupleBufferResult::eos();
        }
        /// Timeout expired with no data — caller will re-invoke.
        return FillTupleBufferResult::eos();
    }

    /// Write as many requests as fit into the tuple buffer using schema-aware field offsets.
    const size_t tupleSize = schema->getSizeOfSchemaInBytes();
    size_t bytesWritten = 0;
    auto* buffer = tupleBuffer.getAvailableMemoryArea().data();
    const size_t capacity = tupleBuffer.getBufferSize();

    while (!requestQueue.empty() && (bytesWritten + tupleSize) <= capacity)
    {
        auto req = requestQueue.front();
        requestQueue.pop();

        /// Map request fields to schema field values by name.
        auto* tupleStart = buffer + bytesWritten;
        size_t fieldOffset = 0;
        for (size_t i = 0; i < schema->getNumberOfFields(); i++)
        {
            const auto& field = schema->getFieldAt(i);
            const auto fieldSize = field.dataType.getSizeInBytesWithNull();

            uint64_t value = 0;
            if (field.name.find("STATISTICID") != std::string::npos)
            {
                value = req.statisticId;
            }
            else if (field.name.find("STATISTICSTART") != std::string::npos)
            {
                value = req.startTs;
            }
            else if (field.name.find("STATISTICEND") != std::string::npos)
            {
                value = req.endTs;
            }

            std::memcpy(tupleStart + fieldOffset, &value, fieldSize);
            fieldOffset += fieldSize;
        }

        bytesWritten += tupleSize;
    }

    return FillTupleBufferResult::withBytes(bytesWritten);
}

void GrpcSource::enqueueRequest(PendingStatisticRequest request)
{
    {
        const std::lock_guard lock{queueMutex};
        requestQueue.push(request);
    }
    queueCv.notify_one();
}

void GrpcSource::close()
{
    NES_DEBUG("GrpcSource::close: Shutting down gRPC server.");
    if (grpcServer)
    {
        grpcServer->Shutdown();
        grpcServer.reset();
    }
}

std::ostream& GrpcSource::toString(std::ostream& str) const
{
    str << "GrpcSource(port=" << actualPort << ", receiveTimeoutMs=" << receiveTimeoutMs << ")";
    return str;
}

DescriptorConfig::Config GrpcSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersGrpcSource>(std::move(config), name());
}

SourceValidationRegistryReturnType RegisterGrpcSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return GrpcSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterGrpcSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<GrpcSource>(sourceRegistryArguments.sourceDescriptor);
}

}
