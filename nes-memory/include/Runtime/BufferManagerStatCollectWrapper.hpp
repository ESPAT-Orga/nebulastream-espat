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
#include <optional>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManagerStatisticListener.hpp>

namespace NES
{
class BufferManager;
struct BufferManagerStatisticListener;

/**
 * @brief This class wraps the buffer manager and collects statistics whenever buffers are obtained/returned
 */
class BufferManagerStatCollectWrapper final : public std::enable_shared_from_this<BufferManagerStatCollectWrapper>,
                                              public AbstractBufferProvider
{
public:
    explicit BufferManagerStatCollectWrapper(std::shared_ptr<BufferManager> bufferManager, BufferCreatorId creatorId);

    BufferManagerStatCollectWrapper(const BufferManager&) = delete;
    BufferManagerStatCollectWrapper& operator=(const BufferManager&) = delete;
    ~BufferManagerStatCollectWrapper() override;

public:
    /// This blocks until a buffer is available.
    TupleBuffer getBufferBlocking() override;

    /// invalid optional if there is no buffer.
    std::optional<TupleBuffer> getBufferNoBlocking() override;

    /**
     * @brief Returns a new Buffer wrapped in an optional or an invalid option if there is no buffer available within
     * timeoutMs.
     * @param timeoutMs the amount of time to wait for a new buffer to be retuned
     * @param creatorId the source or pipeline requesting the buffer
     * @return a new buffer
     */
    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeoutMs) override;

    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) override;
    size_t getBufferSize() const override;
    size_t getNumOfPooledBuffers() const override;
    size_t getNumOfUnpooledBuffers() const override;
    BufferManagerType getBufferManagerType() const override;

private:
    void collectPooledBufferStatistics(TupleBuffer buffer);
    std::shared_ptr<BufferManager> bufferManager;
    BufferCreatorId creatorId;
};
}
