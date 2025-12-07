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
#include <Runtime/BufferManagerStatCollectWrapper.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferManagerStatisticListener.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

BufferManagerStatCollectWrapper::BufferManagerStatCollectWrapper(std::shared_ptr<BufferManager> bufferManager, BufferCreatorId creatorId)
    : bufferManager(bufferManager), creatorId(creatorId)
{
}

BufferManagerStatCollectWrapper::~BufferManagerStatCollectWrapper()
{
}

void BufferManagerStatCollectWrapper::collectPooledBufferStatistics(TupleBuffer buffer)
{
    auto statistic = bufferManager->getBufferManagerStatisticListener();
    if (statistic)
    {
        INVARIANT(creatorId.has_value(), "Recycling buffer callback invoked on used memory segment");
        statistic->onEvent(GetPooledBufferEvent(buffer.getBufferSize(), creatorId));
        buffer.setRecycleStatisticsCallback([statistic, size = buffer.getBufferSize(), creatorId = this->creatorId](detail::MemorySegment*)
                                            { statistic->onEvent(RecyclePooledBufferEvent(size, creatorId)); });
    }
}

TupleBuffer BufferManagerStatCollectWrapper::getBufferBlocking()
{
    auto buffer = bufferManager->getBufferBlocking();
    collectPooledBufferStatistics(buffer);
    return buffer;
}

std::optional<TupleBuffer> BufferManagerStatCollectWrapper::getBufferNoBlocking()
{
    auto buffer = bufferManager->getBufferNoBlocking();
    if (buffer)
    {
        collectPooledBufferStatistics(buffer.value());
    }
    return buffer;
}

std::optional<TupleBuffer> BufferManagerStatCollectWrapper::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs)
{
    auto buffer = bufferManager->getBufferWithTimeout(timeoutMs);
    if (buffer)
    {
        collectPooledBufferStatistics(buffer.value());
    }
    return buffer;
}

std::optional<TupleBuffer> BufferManagerStatCollectWrapper::getUnpooledBuffer(const size_t bufferSize)
{
    auto buffer = bufferManager->getUnpooledBuffer(bufferSize);
    auto statistic = bufferManager->getBufferManagerStatisticListener();
    if (buffer && statistic)
    {
        statistic->onEvent(GetUnpooledBufferEvent(buffer->getBufferSize(), creatorId));
        INVARIANT(creatorId.has_value(), "Recycling buffer callback invoked on used memory segment");
        buffer->setRecycleStatisticsCallback(
            [statistic, size = buffer->getBufferSize(), creatorId = this->creatorId](detail::MemorySegment*)
            { statistic->onEvent(RecycleUnpooledBufferEvent(size, creatorId)); });
    }
    return buffer;
}

size_t BufferManagerStatCollectWrapper::getBufferSize() const
{
    return bufferManager->getBufferSize();
}

size_t BufferManagerStatCollectWrapper::getNumOfPooledBuffers() const
{
    return bufferManager->getNumOfPooledBuffers();
}

size_t BufferManagerStatCollectWrapper::getNumOfUnpooledBuffers() const
{
    return bufferManager->getNumOfUnpooledBuffers();
}

BufferManagerType BufferManagerStatCollectWrapper::getBufferManagerType() const
{
    return bufferManager->getBufferManagerType();
}
}
