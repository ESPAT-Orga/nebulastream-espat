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

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/UnpooledChunksManager.hpp>
#include <folly/MPMCQueue.h>

#include "BufferManager.hpp"

namespace NES
{
struct BufferManagerStatisticListener;

/**
 * @brief The BufferManager is responsible for:
 * 1. Pooled Buffers: preallocated fixed-size buffers of memory that must be reference counted
 * 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly. They are also subject to reference
 * counting.
 *
 * The reference counting mechanism of the TupleBuffer is explained in TupleBuffer.hpp
 *
 * The BufferManager stores the pooled buffers as MemorySegment-s. When a component asks for a Pooled buffer,
 * then the BufferManager retrieves an available buffer (it blocks the calling thread, if no buffer is available).
 * It then hands out a TupleBuffer that is constructed through the pointer stored inside a MemorySegment.
 * This is necessary because the BufferManager must keep all buffers stored to ensure that when its
 * destructor is called, all buffers that it has ever created are deallocated. Note the BufferManager will check also
 * that no reference counter is non-zero and will throw a fatal exception, if a component hasnt returned every buffers.
 * This is necessary to avoid memory leaks.
 *
 * Unpooled buffers are either allocated on the spot or served via a previously allocated, unpooled buffer that has
 * been returned to the BufferManager by some component.
 *
 */
class BufferManagerStatCollectWrapper final : public std::enable_shared_from_this<BufferManagerStatCollectWrapper>, public AbstractBufferProvider
{

public:
    explicit BufferManagerStatCollectWrapper(
        std::shared_ptr<NES::AbstractBufferProvider> bufferManager,
        BufferCreatorId creatorId );

    /// Creates a new global buffer manager
    /// @param bufferSize
    /// @param numOfBuffers
    /// @param statistic
    /// @param memoryResource
    /// @param withAlignment
    /// @param creatorId the id of the pipeline or source using this object
    // static std::shared_ptr<BufferManager> create(
    //     uint32_t bufferSize,
    //     uint32_t numOfBuffers,
    //     std::shared_ptr<BufferManagerStatisticListener> statistic,
    //     const std::shared_ptr<std::pmr::memory_resource>& memoryResource,
    //     uint32_t withAlignment,
    //     BufferCreatorId creatorId);

    BufferManagerStatCollectWrapper(const BufferManager&) = delete;
    BufferManagerStatCollectWrapper& operator=(const BufferManager&) = delete;
    ~BufferManagerStatCollectWrapper() override;

public:
    /// This blocks until a buffer is available.
    TupleBuffer getBufferBlocking(BufferCreatorId creatorId = std::nullopt) override;

    /// invalid optional if there is no buffer.
    std::optional<TupleBuffer> getBufferNoBlocking(BufferCreatorId creatorId = std::nullopt) override;

    /**
     * @brief Returns a new Buffer wrapped in an optional or an invalid option if there is no buffer available within
     * timeoutMs.
     * @param timeoutMs the amount of time to wait for a new buffer to be retuned
     * @param creatorId the source or pipeline requesting the buffer
     * @return a new buffer
     */
    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeoutMs, BufferCreatorId creatorId = std::nullopt) override;

    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize, BufferCreatorId creatorId = std::nullopt) override;
    size_t getBufferSize() const override;
    size_t getNumOfPooledBuffers() const override;
    size_t getNumOfUnpooledBuffers() const override;
    BufferManagerType getBufferManagerType() const override;

    //TODO pull the statistics collection completely into this class, so that we do not have to change anything in the buffer manage itself

private:
    std::shared_ptr<AbstractBufferProvider> bufferManager;
    BufferCreatorId creatorId;
    std::shared_ptr<BufferManagerStatisticListener> statistic;
};


}
