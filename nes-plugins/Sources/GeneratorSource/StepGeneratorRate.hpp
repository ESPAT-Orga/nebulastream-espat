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
#include <optional>
#include <string_view>
#include <tuple>
#include <GeneratorRate.hpp>

namespace NES
{

/// Provides a square-wave (step) emit rate alternating between two configurable rates with
/// configurable durations for each phase. During the "high" phase the source emits at highRate
/// tuples/s for periodHighSeconds; during the "low" phase it emits at lowRate tuples/s for
/// periodLowSeconds. The cycle repeats indefinitely; phase is anchored to the system_clock epoch
/// so the same wall-clock instant always falls in the same phase across queries on the same host.
class StepGeneratorRate final : public GeneratorRate
{
    double highRate = 0;
    double lowRate = 0;
    double periodHighSeconds = 0;
    double periodLowSeconds = 0;

public:
    /// Tries to parse a config string of the form
    ///   "high_rate <N>, low_rate <N>, period_high <SECONDS>, period_low <SECONDS>"
    /// (key/value separator is whitespace, pairs are comma-separated; key order is not significant).
    /// Returns (highRate, lowRate, periodHighSeconds, periodLowSeconds) on success.
    static std::optional<std::tuple<double, double, double, double>> parseAndValidateConfigString(std::string_view configString);
    explicit StepGeneratorRate(double highRate, double lowRate, double periodHighSeconds, double periodLowSeconds);
    ~StepGeneratorRate() override = default;
    uint64_t calcNumberOfTuplesForInterval(
        const std::chrono::time_point<std::chrono::system_clock>& start,
        const std::chrono::time_point<std::chrono::system_clock>& end) override;
};
}
