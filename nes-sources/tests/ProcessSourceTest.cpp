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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <ProcessSource.hpp>

namespace NES
{

namespace
{
constexpr size_t RANDOM_GENERATOR_SEED = 42;

std::string readFromSource(ProcessSource& source, AbstractBufferProvider& bm, const std::stop_token& stopToken)
{
    std::string result;
    auto buffer = bm.getBufferBlocking();
    auto fillResult = source.fillTupleBuffer(buffer, stopToken);
    while (not fillResult.isEoS())
    {
        result.append(buffer.getAvailableMemoryArea<char>().data(), fillResult.getNumberOfBytes());
        buffer = bm.getBufferBlocking();
        fillResult = source.fillTupleBuffer(buffer, stopToken);
    }
    return result;
}

void writeToPipe(const int writeFd, const std::string& data)
{
    size_t offset = 0;
    while (offset < data.size())
    {
        const auto written = write(writeFd, data.data() + offset, data.size() - offset);
        ASSERT_GT(written, 0) << "write() failed";
        offset += written;
    }
}

std::string generateRandomString(const size_t length, const uint32_t seed)
{
    std::mt19937 rnd{seed};
    std::uniform_int_distribution dist{32, 126};
    std::string data;
    data.reserve(length);
    for (size_t i = 0; i < length; ++i)
    {
        data.push_back(static_cast<char>(dist(rnd)));
    }
    return data;
}
}

class ProcessSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ProcessSourceTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override
    {
        if (not pipePath.empty() && std::filesystem::exists(pipePath))
        {
            std::filesystem::remove(pipePath);
        }
        BaseUnitTest::TearDown();
    }

    std::string createTmpPipe()
    {
        pipePath = std::filesystem::temp_directory_path() / ("nes_test_pipe_" + std::to_string(getpid()));
        if (std::filesystem::exists(pipePath))
        {
            std::filesystem::remove(pipePath);
        }
        EXPECT_EQ(mkfifo(pipePath.c_str(), 0600), 0) << "mkfifo failed: " << std::strerror(errno);
        return pipePath;
    }

    static SourceDescriptor createProcessSourceDescriptor(const std::string& path, std::unordered_map<std::string, std::string> config = {})
    {
        auto catalog = SourceCatalog{};
        auto schema = Schema{};
        /// The schema is irrelevant for this test as we write and read raw bytes. It is just required by the SourceDescriptor
        schema.addField("value", DataTypeProvider::provideDataType(DataType::Type::INT32));
        const auto logicalSource = catalog.addLogicalSource("testSource", schema);
        EXPECT_TRUE(logicalSource.has_value());
        config.try_emplace("pipe_path", path);
        auto descriptor = catalog.addPhysicalSource(*logicalSource, "Process", std::move(config), {{"type", "CSV"}});
        EXPECT_TRUE(descriptor.has_value());
        return std::move(descriptor.value());
    }

protected:
    std::string pipePath;
};

TEST_F(ProcessSourceTest, SimpleReadFromPipe)
{
    const auto path = createTmpPipe();
    const auto descriptor = createProcessSourceDescriptor(path);
    auto source = ProcessSource{descriptor};

    const std::string testData = "1\n2\n3\n";

    auto writerThread = std::thread(
        [&path, &testData]
        {
            const int writeFd = open(path.c_str(), O_WRONLY);
            ASSERT_GE(writeFd, 0);
            writeToPipe(writeFd, testData);
            close(writeFd);
        });

    const auto bm = BufferManager::create();
    source.open(bm);

    const std::stop_source stopSource;
    const auto received = readFromSource(source, *bm, stopSource.get_token());
    EXPECT_EQ(received, testData);

    source.close();
    writerThread.join();
}

TEST_F(ProcessSourceTest, LargeMessageReadFromPipe)
{
    const auto path = createTmpPipe();
    const auto descriptor = createProcessSourceDescriptor(path);
    auto source = ProcessSource{descriptor};

    /// Generate a message larger than the default TupleBuffer size (8192 bytes)
    const std::string testData = generateRandomString(100'000, RANDOM_GENERATOR_SEED);

    auto writerThread = std::thread(
        [&path, &testData]
        {
            const int writeFd = open(path.c_str(), O_WRONLY);
            ASSERT_GE(writeFd, 0);
            writeToPipe(writeFd, testData);
            close(writeFd);
        });

    const auto bm = BufferManager::create();
    source.open(bm);

    const std::stop_source stopSource;
    const auto received = readFromSource(source, *bm, stopSource.get_token());
    EXPECT_EQ(received.size(), testData.size());
    EXPECT_EQ(received, testData);

    source.close();
    writerThread.join();
}

TEST_F(ProcessSourceTest, MultipleMessagesNoDelay)
{
    const auto path = createTmpPipe();
    const auto descriptor = createProcessSourceDescriptor(path);
    auto source = ProcessSource{descriptor};

    const std::vector<std::string> messages = {"first\n", "second\n", "third\n", "fourth\n", "fifth\n"};
    std::string expectedAll;
    for (const auto& msg : messages)
    {
        expectedAll += msg;
    }

    auto writerThread = std::thread(
        [&path, &messages]
        {
            const int writeFd = open(path.c_str(), O_WRONLY);
            ASSERT_GE(writeFd, 0);
            for (const auto& msg : messages)
            {
                writeToPipe(writeFd, msg);
            }
            close(writeFd);
        });

    const auto bm = BufferManager::create();
    source.open(bm);

    const std::stop_source stopSource;
    const auto received = readFromSource(source, *bm, stopSource.get_token());
    EXPECT_EQ(received, expectedAll);

    source.close();
    writerThread.join();
}

TEST_F(ProcessSourceTest, MultipleMessagesWithDelay)
{
    const auto path = createTmpPipe();
    const auto descriptor = createProcessSourceDescriptor(path);
    auto source = ProcessSource{descriptor};

    const std::vector<std::string> messages = {"first\n", "second\n", "third\n", "fourth\n", "fifth\n"};
    std::string expectedAll;
    for (const auto& msg : messages)
    {
        expectedAll += msg;
    }

    auto writerThread = std::thread(
        [&path, &messages]
        {
            const int writeFd = open(path.c_str(), O_WRONLY);
            ASSERT_GE(writeFd, 0);
            for (const auto& msg : messages)
            {
                writeToPipe(writeFd, msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            close(writeFd);
        });

    const auto bm = BufferManager::create();
    source.open(bm);

    const std::stop_source stopSource;
    const auto received = readFromSource(source, *bm, stopSource.get_token());
    EXPECT_EQ(received, expectedAll);

    source.close();
    writerThread.join();
}

TEST_F(ProcessSourceTest, MultipleMessagesRandomContent)
{
    const auto path = createTmpPipe();
    const auto descriptor = createProcessSourceDescriptor(path);
    auto source = ProcessSource{descriptor};

    /// Generate random data of various sizes and concatenate them
    const std::vector<size_t> chunkSizes = {1, 7, 63, 4096, 8191, 8192, 8193, 16384, 50000};
    std::string expectedAll;
    for (size_t i = 0; i < chunkSizes.size(); ++i)
    {
        expectedAll += generateRandomString(chunkSizes[i], RANDOM_GENERATOR_SEED + i);
    }

    auto writerThread = std::thread(
        [&path, &expectedAll]
        {
            const int writeFd = open(path.c_str(), O_WRONLY);
            ASSERT_GE(writeFd, 0);
            writeToPipe(writeFd, expectedAll);
            close(writeFd);
        });

    const auto bm = BufferManager::create();
    source.open(bm);

    const std::stop_source stopSource;
    const auto received = readFromSource(source, *bm, stopSource.get_token());
    EXPECT_EQ(received.size(), expectedAll.size());
    EXPECT_EQ(received, expectedAll);

    source.close();
    writerThread.join();
}

TEST_F(ProcessSourceTest, StopTokenResponsiveness)
{
    const auto path = createTmpPipe();
    /// Use a small poll timeout so the test completes faster
    const auto descriptor = createProcessSourceDescriptor(path, {{"poll_timeout_ms", "10"}});
    auto source = ProcessSource{descriptor};

    /// Writer thread keeps the pipe open without writing, so fillTupleBuffer blocks on poll()
    auto writerThread = std::thread(
        [&path]
        {
            const int writeFd = open(path.c_str(), O_WRONLY);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            close(writeFd);
        });

    const auto bm = BufferManager::create();
    source.open(bm);

    std::stop_source stopSource;
    auto buffer = bm->getBufferBlocking();

    auto fillThread = std::thread(
        [&source, &buffer, &stopSource]
        {
            const auto result = source.fillTupleBuffer(buffer, stopSource.get_token());
            EXPECT_TRUE(result.isEoS());
        });

    /// Give fillTupleBuffer time to enter poll loop, then request stop
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto beforeStop = std::chrono::high_resolution_clock::now();
    stopSource.request_stop();
    fillThread.join();
    const auto afterStop = std::chrono::high_resolution_clock::now();

    /// With poll_timeout_ms=10, stop should return well within 50ms
    EXPECT_LT(afterStop - beforeStop, std::chrono::milliseconds(50));

    source.close();
    writerThread.join();
}

TEST_F(ProcessSourceTest, FlushIntervalAccumulatesMultipleWrites)
{
    const auto path = createTmpPipe();
    /// flush_interval_ms=500 is much larger than the 50ms delay between writes, so a single fillTupleBuffer call should accumulate all messages
    const auto descriptor = createProcessSourceDescriptor(path, {{"flush_interval_ms", "500"}});
    auto source = ProcessSource{descriptor};

    const std::vector<std::string> messages = {"first\n", "second\n", "third\n", "fourth\n", "fifth\n"};
    std::string expectedAll;
    for (const auto& msg : messages)
    {
        expectedAll += msg;
    }

    auto writerThread = std::thread(
        [&path, &messages]
        {
            const int writeFd = open(path.c_str(), O_WRONLY);
            ASSERT_GE(writeFd, 0);
            for (const auto& msg : messages)
            {
                writeToPipe(writeFd, msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            close(writeFd);
        });

    const auto bm = BufferManager::create();
    source.open(bm);

    /// Count how many fillTupleBuffer calls are needed to drain
    const std::stop_source stopSource;
    size_t fillCalls = 0;
    std::string received;
    auto buffer = bm->getBufferBlocking();
    auto result = source.fillTupleBuffer(buffer, stopSource.get_token());
    while (not result.isEoS())
    {
        ++fillCalls;
        received.append(buffer.getAvailableMemoryArea<char>().data(), result.getNumberOfBytes());
        buffer = bm->getBufferBlocking();
        result = source.fillTupleBuffer(buffer, stopSource.get_token());
    }

    EXPECT_EQ(received, expectedAll);
    EXPECT_EQ(fillCalls, 1) << "Expected flush interval to accumulate all writes into one fillTupleBuffer call";

    source.close();
    writerThread.join();
}

TEST_F(ProcessSourceTest, OpenNonExistentPipe)
{
    const auto descriptor = createProcessSourceDescriptor("/tmp/non_existent_pipe_that_does_not_exist_12345");
    auto source = ProcessSource{descriptor};
    const auto bm = BufferManager::create();
    EXPECT_THROW(source.open(bm), Exception);
}

}
