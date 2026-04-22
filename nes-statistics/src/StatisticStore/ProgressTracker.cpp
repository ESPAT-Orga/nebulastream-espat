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

#include <ProgressTracker.hpp>
#include <StatisticStoreBenchmarkUtils.hpp>

#include <iomanip>
#include <iostream>
#include <sstream>

namespace NES
{
namespace
{

/// Formats a duration in seconds as a double string with 1 decimal place.
std::string formatSeconds(const double seconds)
{
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(4) << seconds << "s";
    return oss.str();
}


}

ProgressTracker::ProgressTracker(const uint64_t totalConfigs)
    : totalConfigs(totalConfigs), startTime(std::chrono::steady_clock::now()), lastReportTime(startTime)
{
}

void ProgressTracker::beginSection(const uint64_t sectionConfigs)
{
    sectionTotal = sectionConfigs;
    sectionCompleted = 0;
}

void ProgressTracker::report(const std::string& configDesc, const std::string& skippedReason)
{
    ++completedConfigs;
    ++sectionCompleted;
    const auto now = std::chrono::steady_clock::now();
    const double configSec = std::chrono::duration<double>(now - lastReportTime).count();
    const double elapsedSec = std::chrono::duration<double>(now - startTime).count();
    lastReportTime = now;

    const double avgSecPerConfig = elapsedSec / static_cast<double>(completedConfigs);
    const double etaSec = avgSecPerConfig * static_cast<double>(totalConfigs - completedConfigs);

    /// For skipped configs, overwrite the current line in-place with "Skipping: ..." and return.
    /// No newline is printed, so the next report() call overwrites this line (either with another
    /// "Skipping: ..." or with a permanent config result + progress bar).
    if (not skippedReason.empty())
    {
        std::cout << "\r\033[2K" << "Skipping: " << configDesc << " (" << skippedReason << ")" << "\r" << std::flush;
        return;
    }

    /// Clear the progress bar line, print config result above it as a permanent line
    std::cout << "\r\033[2K" << configDesc << "  " << formatSeconds(configSec) << "\n";

    /// Draw the progress bar scoped to the current section
    const int filled = sectionTotal > 0 ? static_cast<int>(BAR_WIDTH * sectionCompleted / sectionTotal) : 0;
    std::cout << "[";
    for (int i = 0; i < BAR_WIDTH; ++i)
    {
        if (i < filled)
        {
            std::cout << '=';
        }
        else if (i == filled)
        {
            std::cout << '>';
        }
        else
        {
            std::cout << ' ';
        }
    }
    std::cout << "] " << sectionCompleted << "/" << sectionTotal << "  elapsed " << formatHMS(elapsedSec) << "  ETA " << formatHMS(etaSec)
              << "\r" << std::flush;
}

void ProgressTracker::skip(const std::string& benchmark, const std::string& description, const std::string& reason)
{
    skippedConfigs.emplace_back(benchmark, description, reason);
}

void ProgressTracker::printSkipped() const
{
    if (skippedConfigs.empty())
    {
        return;
    }

    std::cout << "\n--- Skipped configs (" << skippedConfigs.size() << ") ---\n";
    for (const auto& [benchmark, description, reason] : skippedConfigs)
    {
        std::cout << "  " << benchmark << " | " << description << "\n"
                  << "    reason: " << reason << "\n";
    }
}

void ProgressTracker::clearProgressBar()
{
    std::cout << "\r\033[2K" << std::flush;
}

void ProgressTracker::finalizeProgressBar()
{
    const double elapsedSec = std::chrono::duration<double>(std::chrono::steady_clock::now() - startTime).count();
    const double avgSecPerConfig = completedConfigs > 0 ? elapsedSec / static_cast<double>(completedConfigs) : 0;
    const double etaSec = avgSecPerConfig * static_cast<double>(totalConfigs - completedConfigs);

    const int filled = sectionTotal > 0 ? static_cast<int>(BAR_WIDTH * sectionCompleted / sectionTotal) : 0;
    std::cout << "\r\033[2K[";
    for (int i = 0; i < BAR_WIDTH; ++i)
    {
        if (i < filled)
        {
            std::cout << '=';
        }
        else if (i == filled)
        {
            std::cout << '>';
        }
        else
        {
            std::cout << ' ';
        }
    }
    std::cout << "] " << sectionCompleted << "/" << sectionTotal << "  elapsed " << formatHMS(elapsedSec) << "  ETA " << formatHMS(etaSec)
              << "\n";
}

double ProgressTracker::getElapsedSeconds() const
{
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - startTime).count();
}

}
