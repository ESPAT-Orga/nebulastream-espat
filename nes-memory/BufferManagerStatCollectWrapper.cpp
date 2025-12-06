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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <utility>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <gmock/internal/gmock-internal-utils.h>

#include <Runtime/BufferManagerStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{

BufferManagerStatCollectWrapper::BufferManagerStatCollectWrapper(
    Private p,
    const uint32_t bufferSize,
    const uint32_t numOfBuffers,
    std::shared_ptr<BufferManagerStatisticListener> statistic,
    std::shared_ptr<std::pmr::memory_resource> memoryResource,
    const uint32_t withAlignment, BufferCreatorId bufferCreatorId)
: BufferManager(p, bufferSize, numOfBuffers, statistic, memoryResource, withAlignment), creatorId(bufferCreatorId)
{
}

std::shared_ptr<BufferManager> BufferManagerStatCollectWrapper::create(
    uint32_t bufferSize,
    uint32_t numOfBuffers,
    std::shared_ptr<BufferManagerStatisticListener> statistic,
    const std::shared_ptr<std::pmr::memory_resource>& memoryResource,
    uint32_t withAlignment, BufferCreatorId creatorId)
{
    return std::make_shared<BufferManagerStatCollectWrapper>(Private{}, bufferSize, numOfBuffers, statistic, memoryResource, withAlignment, creatorId);
}

TupleBuffer BufferManagerStatCollectWrapper::getBufferBlocking(BufferCreatorId)
{
    return BufferManager::getBufferBlocking(this->creatorId);
}

std::optional<TupleBuffer> BufferManagerStatCollectWrapper::getBufferNoBlocking(BufferCreatorId)
{
    return BufferManager::getBufferNoBlocking(this->creatorId);

}

std::optional<TupleBuffer> BufferManagerStatCollectWrapper::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs, BufferCreatorId)
{
    return BufferManager::getBufferWithTimeout(timeoutMs, this->creatorId);
}

std::optional<TupleBuffer> BufferManagerStatCollectWrapper::getUnpooledBuffer(const size_t bufferSize, BufferCreatorId)
{
    return BufferManager::getUnpooledBuffer(bufferSize, this->creatorId);
}
}
