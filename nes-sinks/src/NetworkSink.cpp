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

#include <Sinks/NetworkSink.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategy.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <network/lib.h>
#include <rust/cxx.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <QueryId.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

BackpressureHandler::BackpressureHandler(size_t upperThreshold, size_t lowerThreshold)
    : upperThreshold(upperThreshold), lowerThreshold(lowerThreshold)
{
    if (this->lowerThreshold > this->upperThreshold)
    {
        NES_WARNING("Lower threshold is greater than upper threshold. Setting lower threshold to upper threshold.");
        std::swap(this->lowerThreshold, this->upperThreshold);
    }
}

BackpressureHandler::OnFullResult BackpressureHandler::onFull(TupleBuffer buffer, BackpressureController& backpressureController)
{
    auto rstate = stateLock.ulock();

    /// If this is the pending retry buffer, re-emit it to keep the retry loop alive.
    if (buffer.getSequenceNumber() == rstate->pendingSequenceNumber && buffer.getChunkNumber() == rstate->pendingChunkNumber)
    {
        return {.retryBuffer = std::move(buffer), .didApplyBackpressure = false};
    }

    const auto wstate = rstate.moveFromUpgradeToWrite();
    wstate->buffered.emplace_back(std::move(buffer));

    /// Apply backpressure when the buffer count reaches the upper hysteresis threshold.
    bool didApplyBackpressure = false;
    if (!wstate->hasBackpressure && wstate->buffered.size() >= upperThreshold)
    {
        backpressureController.applyPressure();
        NES_DEBUG("Backpressure acquired: {} buffered (upper threshold: {})", wstate->buffered.size(), upperThreshold);
        wstate->hasBackpressure = true;
        didApplyBackpressure = true;
    }

    /// Ensure there is always one pending buffer being retried to avoid deadlocks.
    if (wstate->pendingSequenceNumber == INVALID<SequenceNumber>)
    {
        auto pending = std::move(wstate->buffered.front());
        wstate->buffered.pop_front();
        wstate->pendingSequenceNumber = pending.getSequenceNumber();
        wstate->pendingChunkNumber = pending.getChunkNumber();
        return {.retryBuffer = std::move(pending), .didApplyBackpressure = didApplyBackpressure};
    }

    return {.retryBuffer = std::nullopt, .didApplyBackpressure = didApplyBackpressure};
}

/// Called on a successful send of a buffer to the network channel.
/// Clears the pending buffer and releases backpressure when the buffer count drops to the lower hysteresis threshold.
/// Returns the next buffered tuple to send, if any.
BackpressureHandler::OnSuccessResult BackpressureHandler::onSuccess(BackpressureController& backpressureController)
{
    const auto state = stateLock.wlock();
    state->pendingSequenceNumber = INVALID<SequenceNumber>;
    state->pendingChunkNumber = INVALID<ChunkNumber>;

    /// Release backpressure when the buffer count drops to the lower hysteresis threshold.
    bool didReleaseBackpressure = false;
    if (state->hasBackpressure && state->buffered.size() <= lowerThreshold)
    {
        backpressureController.releasePressure();
        NES_DEBUG("Backpressure released: {} buffered (lower threshold: {})", state->buffered.size(), lowerThreshold);
        state->hasBackpressure = false;
        didReleaseBackpressure = true;
    }

    if (not state->buffered.empty())
    {
        auto nextBuffer = std::move(state->buffered.front());
        state->buffered.pop_front();
        return {.nextBuffer = std::move(nextBuffer), .didReleaseBackpressure = didReleaseBackpressure};
    }
    return {.nextBuffer = std::nullopt, .didReleaseBackpressure = didReleaseBackpressure};
}

bool BackpressureHandler::empty() const
{
    return stateLock.rlock()->buffered.empty();
}

NetworkSink::NetworkSink(
    BackpressureController backpressureController,
    const SinkDescriptor& sinkDescriptor,
    QueryId queryId,
    Priority priority,
    std::shared_ptr<NetworkSinkSendingStrategy> sendingStrategy)
    : Sink(std::move(backpressureController))
    , tupleSize(sinkDescriptor.getSchema()->getSizeOfSchemaInBytes())
    , backpressureHandler(
          sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::BACKPRESSURE_UPPER_THRESHOLD),
          sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::BACKPRESSURE_LOWER_THRESHOLD))
    , channelId(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::CHANNEL))
    , connectionAddr(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::DATA_ENDPOINT))
    , thisConnection(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::BIND))
    , senderQueueSize(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::SENDER_QUEUE_SIZE))
    , maxPendingAcks(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::MAX_PENDING_ACKS))
    , queryId(std::move(queryId))
    , priority(std::move(priority))
    , sendingStrategy(std::move(sendingStrategy))
{
    PRECONDITION(this->sendingStrategy != nullptr, "NetworkSink requires a non-null NetworkSinkSendingStrategy");
}

void NetworkSink::start(PipelineExecutionContext&)
{
    this->server = sender_instance(thisConnection);
    const NetworkServiceOptions options{
        .sender_queue_size = static_cast<uint32_t>(senderQueueSize),
        .max_pending_acks = static_cast<uint32_t>(maxPendingAcks),
        .receiver_queue_size = 0,
    };
    this->channel = register_sender_channel(*server.value(), connectionAddr, rust::String(channelId), options);
    sendingStrategy->registerChannel(queryId, priority);
    NES_DEBUG("Sender channel registered: {} (priority={})", channelId, priority);
}

void NetworkSink::stop(PipelineExecutionContext& pec)
{
    PRECONDITION(channel, "Sender channel is not initialized");
    if (!closed)
    {
        INVARIANT(backpressureHandler.empty(), "BackpressureHandler is not empty");

        /// Check if the sender network service has pending buffers to send
        /// If yes, keep the pipeline alive by emitting an empty buffer
        if (!flush_sender_channel(*this->channel.value()))
        {
            pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
            return;
        }
    }

    sendingStrategy->deregisterChannel(queryId);
    NES_DEBUG("Closing Sender channel {}", channelId);
    close_sender_channel(*std::move(this->channel));
    NES_DEBUG("Sender channel {} closed", channelId);
}

void NetworkSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(channel, "Sender channel is not initialized");
    PRECONDITION(inputBuffer, "Invalid input buffer in NetworkSink.");

    if (closed)
    {
        NES_WARNING("Sink is closed dropping buffer: {}-{}", inputBuffer.getSequenceNumber(), inputBuffer.getChunkNumber());
        return;
    }

    /// Sojourn-time tracking — stamp the buffer's first-arrival timestamp BEFORE any gating /
    /// retry logic so we measure the full engine-side wait. try_emplace makes this idempotent
    /// across pec.repeatTask retries (same sequence number / origin / chunk arrives each time).
    backpressureController.recordBufferArrival(inputBuffer.getSequenceNumber(), inputBuffer.getOriginId(), inputBuffer.getChunkNumber());

    auto currentBuffer = std::optional(inputBuffer);
    while (currentBuffer)
    {
        /// Set buffer header
        const SerializedTupleBufferHeader metadata{
            .sequence_number = currentBuffer->getSequenceNumber().getRawValue(),
            .origin_id = currentBuffer->getOriginId().getRawValue(),
            .chunk_number = currentBuffer->getChunkNumber().getRawValue(),
            .number_of_tuples = currentBuffer->getNumberOfTuples(),
            .watermark = currentBuffer->getWatermark().getRawValue(),
            .last_chunk = currentBuffer->isLastChunk()};

        /// Set child buffers
        std::vector<rust::Slice<const uint8_t>> children;
        children.reserve(currentBuffer->getNumberOfChildBuffers());
        for (size_t childIdx = 0; childIdx < currentBuffer->getNumberOfChildBuffers(); ++childIdx)
        {
            auto childBuffer = currentBuffer->loadChildBuffer(VariableSizedAccess::Index(childIdx));
            auto childMemory = childBuffer.getAvailableMemoryArea<const uint8_t>();
            /// Send only the bytes the child actually holds, not its allocated capacity. A child that fits in a
            /// pooled buffer IS a whole pooled buffer (TupleBufferRef::getNewBufferForVarSized only falls back to
            /// an exactly-sized unpooled buffer when the value does not fit), so getAvailableMemoryArea() would
            /// put up to operator_buffer_size of padding on the wire for every small variable-sized value. Every
            /// producer of a child buffer records its used byte count in the numberOfTuples field
            /// (TupleBufferRef::writeVarSized, CsvBufferParser, OutputFormatterUtil). A received child is
            /// allocated at exactly the received length and leaves the counter at 0, so 0 means "the whole area
            /// is used" and relayed buffers keep working unchanged.
            const auto usedChildBytes = childBuffer.getNumberOfTuples();
            const auto childPayload
                = (usedChildBytes > 0 and usedChildBytes <= childMemory.size()) ? childMemory.subspan(0, usedChildBytes) : childMemory;
            children.emplace_back(childPayload);
        }

        std::span usedBufferMemory(
            currentBuffer->getAvailableMemoryArea<const uint8_t>().data(), currentBuffer->getNumberOfTuples() * tupleSize);
        const auto dataSlice = rust::Slice(usedBufferMemory);
        const auto childrenSlice = rust::Slice<const rust::Slice<const uint8_t>>(children);
        const auto variant = sendingStrategy->sendVariant(queryId);
        const auto bufferSizeBytes = static_cast<uint64_t>(currentBuffer->getNumberOfTuples() * tupleSize);
        /// Weighted strategy gates per-buffer in C++ via the AdaptiveSendingScheduler's per-
        /// channel contingent. On denial we apply single-buffer pressure to the source and
        /// repeat-task the buffer. We do NOT route through the BackpressureHandler so multiple
        /// buffers don't accumulate while gated.
        if (variant == SendVariant::Weighted && !backpressureController.isScheduledToSend(bufferSizeBytes))
        {
            if (!throttlePressureApplied.exchange(true))
            {
                backpressureController.applyPressure();
            }
            /// Stamp the first deny in a gating episode (compare_exchange leaves an existing
            /// earlier timestamp intact). The matching emit happens below when a later buffer
            /// passes isScheduledToSend.
            const auto nowNs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
            uint64_t expected = 0;
            /// compare-and-set: store only if still 0; updated `expected` intentionally ignored.
            std::ignore = gateDenyStartNs.compare_exchange_strong(expected, nowNs);
            pec.repeatTask(*currentBuffer, BACKPRESSURE_RETRY_INTERVAL);
            return;
        }
        /// Gate passed (or strategy is not Weighted). If a gating episode was in progress, emit
        /// SchedulerGatedEvent with the elapsed nanoseconds and clear. exchange() ensures only
        /// the first buffer to pass after a deny streak fires the event (others see 0).
        if (const uint64_t startNs = gateDenyStartNs.exchange(0); startNs != 0)
        {
            const auto nowNs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
            backpressureController.recordSchedulerGated(nowNs - startNs);
        }
        /// Both Direct and Weighted use the unconditional send_buffer; gating happens above for
        /// Weighted via isScheduledToSend, before this point.
        const auto sendResult = send_buffer(*channel.value(), metadata, dataSlice, childrenSlice);
        switch (sendResult)
        {
            case SendResult::Closed: {
                /// Future buffers are voided.
                this->closed = true;
                [[maybe_unused]] auto droppedBuffer = backpressureHandler.onFull(*currentBuffer, backpressureController);
                /// Buffer will never complete sending — drop its arrival entry so the map doesn't leak.
                backpressureController.forgetBufferArrival(
                    currentBuffer->getSequenceNumber(), currentBuffer->getOriginId(), currentBuffer->getChunkNumber());
                /// Currently there is no way to propagate a query stop without a failure from a sink.
                /// There is no operator that propagates a query stop in upstream direction, so receiving a query stop
                /// from the downstream operator is unexpected, thus failing the query is reasonable.
                throw CannotOpenSink("NetworkSink was closed by other side");
            }
            case SendResult::Ok: {
                NES_TRACE("Sending buffer {}", currentBuffer->getSequenceNumber());
                /// Emit per-send delivered-tuple count for the benchmark.
                backpressureController.recordBufferSent(currentBuffer->getNumberOfTuples());
                /// Engine-side sojourn time: now - first-arrival timestamp. Captures
                /// scheduler-gate retries + retries after send_buffer returned Full (Rust send
                /// queue full) + the send_buffer call. Fires BufferSojournEvent and erases the
                /// in-flight entry.
                backpressureController.recordBufferSojourn(
                    currentBuffer->getSequenceNumber(), currentBuffer->getOriginId(), currentBuffer->getChunkNumber());
                /// Sender-side byte accounting for the AdaptiveSendingScheduler. Decrements
                /// queue_depth_bytes (this buffer left the in-NES queue) and increments
                /// delivered_bytes_last_tick so the next scheduler tick can update the EMA
                /// capacity estimate. No-op when not registered with a scheduler.
                backpressureController.recordBufferSentBytes(bufferSizeBytes);
                sendingStrategy->onBufferSent(queryId, currentBuffer->getNumberOfTuples());
                /// If we previously pressured the source (via either the Full or contingent-denial
                /// path), release it now so the source can resume producing.
                if (throttlePressureApplied.exchange(false))
                {
                    backpressureController.releasePressure();
                    sendingStrategy->onBackpressureReleased(queryId);
                }
                /// Sent a buffer; loop to send the next one (currentBuffer set from upstream
                /// directly — no BackpressureHandler accumulation in the new flow).
                currentBuffer = std::nullopt;
                break;
            }
            case SendResult::Full: {
                /// Same single-buffer-pressure approach as the Weighted contingent-denial path:
                /// pressure the source
                /// immediately and retry just this buffer. This avoids BackpressureHandler
                /// accumulating an arbitrarily large backlog (which the channel would then
                /// drain across multiple wire-times, making the source's STEP-idle phase
                /// invisible at the wire). Use the same flag so the matching releasePressure
                /// fires on the next Ok.
                if (!throttlePressureApplied.exchange(true))
                {
                    backpressureController.applyPressure();
                    sendingStrategy->onBackpressureApplied(queryId);
                }
                pec.repeatTask(*currentBuffer, BACKPRESSURE_RETRY_INTERVAL);
                return;
            }
        }
    }
}

std::ostream& NetworkSink::toString(std::ostream& str) const
{
    return str << fmt::format("NetworkSink(connectionId: {}, channelId: {})", connectionAddr, channelId);
}

DescriptorConfig::Config NetworkSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersNetworkSink>(std::move(config), name());
}

SinkValidationRegistryReturnType RegisterNetworkSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return NetworkSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterNetworkSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<NetworkSink>(
        std::move(sinkRegistryArguments.backpressureController),
        sinkRegistryArguments.sinkDescriptor,
        sinkRegistryArguments.queryId,
        sinkRegistryArguments.priority,
        std::move(sinkRegistryArguments.sendingStrategy));
}

}
