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
#include <StepGeneratorRate.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <Util/Strings.hpp>

namespace NES
{

StepGeneratorRate::StepGeneratorRate(
    const double highRate, const double lowRate, const double periodHighSeconds, const double periodLowSeconds)
    : highRate(highRate), lowRate(lowRate), periodHighSeconds(periodHighSeconds), periodLowSeconds(periodLowSeconds)
{
}

std::optional<std::tuple<double, double, double, double>> StepGeneratorRate::parseAndValidateConfigString(std::string_view configString)
{
    std::unordered_map<std::string, double> parsed;

    for (const auto& param : splitOnMultipleDelimiters(configString, {'\n', ','}))
    {
        const auto trimmed = trimWhiteSpaces(std::string_view(param));
        if (trimmed.empty())
        {
            continue;
        }
        const auto kv = splitWithStringDelimiter<std::string_view>(trimmed, " ");
        if (kv.size() != 2)
        {
            return {};
        }
        const auto value = from_chars<double>(kv[1]);
        if (not value.has_value())
        {
            return {};
        }
        parsed.emplace(toLowerCase(kv[0]), value.value());
    }

    const auto highRate = parsed.find("high_rate");
    const auto lowRate = parsed.find("low_rate");
    const auto periodHigh = parsed.find("period_high");
    const auto periodLow = parsed.find("period_low");

    if (highRate == parsed.end() || lowRate == parsed.end() || periodHigh == parsed.end() || periodLow == parsed.end())
    {
        return {};
    }
    if (periodHigh->second <= 0 || periodLow->second <= 0)
    {
        return {};
    }
    if (highRate->second < 0 || lowRate->second < 0)
    {
        return {};
    }

    return std::make_tuple(highRate->second, lowRate->second, periodHigh->second, periodLow->second);
}

uint64_t StepGeneratorRate::calcNumberOfTuplesForInterval(
    const std::chrono::time_point<std::chrono::system_clock>& start, const std::chrono::time_point<std::chrono::system_clock>& end)
{
    /// Integral of the step function from epoch to t: count complete cycles up to t and add the
    /// partial cycle's contribution. The integral over [start, end] is then F(end) - F(start).
    const auto cyclePeriod = periodHighSeconds + periodLowSeconds;
    if (cyclePeriod <= 0)
    {
        return 0;
    }
    const auto cycleContribution = highRate * periodHighSeconds + lowRate * periodLowSeconds;

    auto integralFromZero = [&](const double t) -> double
    {
        if (t <= 0)
        {
            return 0.0;
        }
        const auto fullCycles = std::floor(t / cyclePeriod);
        const auto remainder = t - fullCycles * cyclePeriod;
        const auto partial = (remainder < periodHighSeconds) ? highRate * remainder
                                                             : highRate * periodHighSeconds + lowRate * (remainder - periodHighSeconds);
        return fullCycles * cycleContribution + partial;
    };

    const auto startTimePoint = std::chrono::duration<double>(start.time_since_epoch()).count();
    const auto endTimePoint = std::chrono::duration<double>(end.time_since_epoch()).count();
    const auto delta = std::max(0.0, integralFromZero(endTimePoint) - integralFromZero(startTimePoint));
    return static_cast<uint64_t>(delta);
}
}
