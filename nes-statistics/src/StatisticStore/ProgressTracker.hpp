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
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace NES
{

struct SkippedConfig
{
    std::string benchmark;
    std::string description;
    std::string reason;
};

/// Tracks and displays benchmark progress across multiple sections (e.g., Insert, Get, Mixed).
///
/// The progress bar is scoped to the current section (reset via beginSection()), while the ETA
/// is computed globally across all sections. Each call to report() prints the completed config
/// description above the progress bar, then redraws the bar in-place.
///
/// Example output during a run:
///
///   --- InsertStatistic (6 configs) ---
///   Insert | DEFAULT | threads=1 | stats=100000 | ids=100000 | size=1024 | ws=60000  1.2s
///   Insert | DEFAULT | threads=4 | stats=100000 | ids=100000 | size=1024 | ws=60000  0.8s
///   [==================>                      ] 2/6  elapsed 00:00:02  ETA 00:00:04
///
/// After finalizeProgressBar():
///
///   [========================================>] 6/6  elapsed 00:00:06  ETA 00:00:00
///   --- InsertStatistic finished in 00:00:06 ---
///
class ProgressTracker
{
public:
    explicit ProgressTracker(uint64_t totalConfigs);

    /// Call at the start of each benchmark function to scope the progress bar to that section.
    void beginSection(uint64_t sectionConfigs);

    /// Prints the config result line, then redraws the progress bar below it.
    /// The progress bar shows per-section progress, the ETA is computed globally.
    /// If skippedReason is non-empty, the config result line is annotated as skipped with that reason
    /// instead of showing the per-config duration.
    void report(const std::string& configDesc, const std::string& skippedReason = "");

    /// Records a skipped config with a reason.
    void skip(const std::string& benchmark, const std::string& description, const std::string& reason);

    /// Prints all skipped configs at the end.
    void printSkipped() const;

    /// Clears the progress bar line. Call before printing section headers or final output.
    void clearProgressBar();

    /// Prints the current section progress bar as a permanent line (with newline).
    void finalizeProgressBar();

    /// Returns the elapsed time since construction in seconds.
    [[nodiscard]] double getElapsedSeconds() const;

private:
    static constexpr int BAR_WIDTH = 40;

    /// Global counters (across all benchmark functions)
    uint64_t totalConfigs;
    uint64_t completedConfigs = 0;

    /// Per-section counters (reset via beginSection)
    uint64_t sectionTotal = 0;
    uint64_t sectionCompleted = 0;

    std::vector<SkippedConfig> skippedConfigs;
    std::chrono::steady_clock::time_point startTime;
    std::chrono::steady_clock::time_point lastReportTime;
};

}
