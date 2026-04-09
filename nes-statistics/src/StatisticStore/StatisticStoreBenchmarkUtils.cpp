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

#include <StatisticStoreBenchmarkUtils.hpp>

#include <iomanip>
#include <iostream>
#include <sstream>

namespace NES
{

ChunkBounds calcChunkBounds(const uint64_t threadId, const uint64_t numItems, const uint64_t numThreads)
{
    const uint64_t baseChunk = numItems / numThreads;
    const uint64_t remainder = numItems % numThreads;
    const uint64_t chunkSize = baseChunk + (threadId < remainder ? 1 : 0);
    const uint64_t start = threadId * baseChunk + std::min(threadId, remainder);
    return {start, start + chunkSize};
}

std::string formatHMS(const double totalSeconds)
{
    const auto totalSec = static_cast<int64_t>(totalSeconds);
    const int64_t hrs = totalSec / 3600;
    const int64_t min = (totalSec % 3600) / 60;
    const int64_t sec = totalSec % 60;
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(2) << hrs << ":" << std::setw(2) << min << ":" << std::setw(2) << sec;
    return oss.str();
}

std::string pad(const std::string_view key, const uint64_t value, const int width)
{
    auto val = std::to_string(value);
    while (static_cast<int>(val.size()) < width)
    {
        val.insert(val.begin(), ' ');
    }
    return std::string{key} + "=" + val;
}

std::string padLeft(const std::string_view value, const int width)
{
    std::string result{value};
    while (static_cast<int>(result.size()) < width)
    {
        result.push_back(' ');
    }
    return result;
}

std::string formatBytes(const uint64_t bytes)
{
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1);
    if (bytes >= 1024ULL * 1024 * 1024)
    {
        oss << static_cast<double>(bytes) / (1024.0 * 1024 * 1024) << " GiB";
    }
    else if (bytes >= 1024ULL * 1024)
    {
        oss << static_cast<double>(bytes) / (1024.0 * 1024) << " MiB";
    }
    else if (bytes >= 1024)
    {
        oss << static_cast<double>(bytes) / 1024.0 << " KiB";
    }
    else
    {
        oss << bytes << " B";
    }
    return oss.str();
}

bool BenchmarkArgs::shouldSkip(const std::string& reportLine) const
{
    for (const auto& word : filter)
    {
        if (not reportLine.contains(word))
        {
            return true;
        }
    }
    for (const auto& word : exclude)
    {
        if (reportLine.contains(word))
        {
            return true;
        }
    }
    return false;
}

BenchmarkArgs::BenchmarkArgs(int argc, char* argv[])
{
    auto printUsage = [&]()
    {
        std::cout << "Usage: " << argv[0] << " [--filter <list>] [--exclude <list>]\n"
                  << "\n"
                  << "Options:\n"
                  << "  --filter <list>  Comma-separated keywords; only configs containing ALL are run.\n"
                  << "                   Example: --filter get,ws=60000\n"
                  << "  --exclude <list> Comma-separated keywords; configs containing ANY are skipped.\n"
                  << "                   Example: --exclude DEFAULT,threads=16\n"
                  << "  --help, -h       Print this help message and exit\n";
    };

    auto parseTokens = [](std::string_view value, std::vector<std::string>& target)
    {
        std::istringstream ss{std::string{value}};
        std::string token;
        while (std::getline(ss, token, ','))
            target.emplace_back(std::move(token));
    };

    for (int i = 1; i < argc; ++i)
    {
        std::string_view arg{argv[i]};

        if (arg == "--help" || arg == "-h")
        {
            printUsage();
            std::exit(0);
        }
        else if (arg == "--filter" || arg == "--exclude")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "Error: " << arg << " requires a value\n";
                printUsage();
                std::exit(1);
            }
            auto& target = (arg == "--filter") ? filter : exclude;
            parseTokens(argv[++i], target);
        }
        else
        {
            std::cerr << "Error: unrecognized argument '" << arg << "'\n";
            printUsage();
            std::exit(1);
        }
    }
}

}
