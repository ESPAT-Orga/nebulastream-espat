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

#include <GrpcSink.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>

namespace NES
{

GrpcSink::GrpcSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , grpcHost(sinkDescriptor.getFromConfig(ConfigParametersGrpcSink::HOST))
    , grpcPort(sinkDescriptor.getFromConfig(ConfigParametersGrpcSink::PORT))
    , schema(sinkDescriptor.getSchema())
{
}

void GrpcSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("GrpcSink::start: Connecting to {}:{}.", grpcHost, grpcPort);
    auto channel = grpc::CreateChannel(grpcHost + ":" + std::to_string(grpcPort), grpc::InsecureChannelCredentials());
    stub = StatisticCoordinatorService::NewStub(channel);
    if (not stub)
    {
        throw CannotOpenSink("GrpcSink: Failed to create gRPC stub for {}:{}", grpcHost, grpcPort);
    }
    NES_INFO("GrpcSink: Connected to {}:{}.", grpcHost, grpcPort);
}

void GrpcSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in GrpcSink.");
    PRECONDITION(stub, "GrpcSink stub not initialized. Was start() called?");

    /// Read tuples from the buffer using schema-aware field offsets. This way is really stupid, as we are not using our query compiler
    /// We choose it here, as we require C++ for the gRPC message.
    const size_t tupleSize = schema->getSizeOfSchemaInBytes();
    const auto* data = inputTupleBuffer.getAvailableMemoryArea<uint8_t>().data();
    const size_t numTuples = inputTupleBuffer.getNumberOfTuples();

    for (size_t t = 0; t < numTuples; t++)
    {
        const auto* tupleStart = data + (t * tupleSize);

        StatisticReport report;
        size_t fieldOffset = 0;
        for (size_t i = 0; i < schema->getNumberOfFields(); i++)
        {
            const auto& field = schema->getFieldAt(i);
            const auto fieldSize = field.dataType.getSizeInBytesWithNull();

            uint64_t value = 0;
            std::memcpy(&value, tupleStart + fieldOffset, fieldSize);

            /// Map schema field names to proto fields.
            if (field.name.find("STATISTICID") != std::string::npos)
            {
                report.set_statistic_id(value);
            }
            else if (field.name.find("STATISTICSTART") != std::string::npos)
            {
                report.set_start_ts(value);
            }
            else if (field.name.find("STATISTICEND") != std::string::npos)
            {
                report.set_end_ts(value);
            }

            fieldOffset += fieldSize;
        }

        grpc::ClientContext context;
        google::protobuf::Empty response;
        auto status = stub->ReportStatistic(&context, report, &response);
        if (not status.ok())
        {
            NES_WARNING("GrpcSink: ReportStatistic failed: {} (code {})", status.error_message(), static_cast<int>(status.error_code()));
        }
    }
}

void GrpcSink::stop(PipelineExecutionContext&)
{
    NES_INFO("GrpcSink: Stopped.");
    stub.reset();
}

DescriptorConfig::Config GrpcSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersGrpcSink>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterGrpcSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return GrpcSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterGrpcSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<GrpcSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
