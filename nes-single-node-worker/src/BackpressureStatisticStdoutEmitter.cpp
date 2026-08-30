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

#include <BackpressureStatisticStdoutEmitter.hpp>

#include <chrono>
#include <iostream>
#include <mutex>
#include <variant>

#include <fmt/format.h>

#include <Util/Overloaded.hpp>
#include <BackpressureStatisticsListener.hpp>

namespace NES
{

namespace
{
long long toNanoseconds(ChronoClock::time_point tp)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch()).count();
}
}

void BackpressureStatisticStdoutEmitter::onEvent(BackpressureEvent event)
{
    std::string line;
    std::visit(
        Overloaded{
            [&](const BufferSentEvent& ev)
            {
                line = fmt::format(
                    "BufferSent for queryId {} priority {} tuples={} at {} ns\n",
                    ev.queryId,
                    ev.priority,
                    ev.numberOfTuples,
                    toNanoseconds(ev.timestamp));
            },
            [&](const BufferIngestEvent& ev)
            {
                line = fmt::format(
                    "BufferIngest for queryId {} priority {} tuples={} at {} ns\n",
                    ev.queryId,
                    ev.priority,
                    ev.numberOfTuples,
                    toNanoseconds(ev.timestamp));
            },
            [&](const ApplyPressureEvent& ev)
            {
                line = fmt::format(
                    "BackpressureApplied for queryId {} priority {} at {} ns\n", ev.queryId, ev.priority, toNanoseconds(ev.timestamp));
            },
            [&](const ReleasePressureEvent& ev)
            {
                line = fmt::format(
                    "BackpressureReleased for queryId {} priority {} at {} ns\n", ev.queryId, ev.priority, toNanoseconds(ev.timestamp));
            },
            [&](const BufferSojournEvent& ev)
            {
                line = fmt::format(
                    "BufferSojourn for queryId {} priority {} sojourn_ns={} at {} ns\n",
                    ev.queryId,
                    ev.priority,
                    ev.sojournNs,
                    toNanoseconds(ev.timestamp));
            },
            [&](const BackpressureBlockedEvent& ev)
            {
                line = fmt::format(
                    "BackpressureBlocked for queryId {} priority {} blocked_ns={} at {} ns\n",
                    ev.queryId,
                    ev.priority,
                    ev.blockedNs,
                    toNanoseconds(ev.timestamp));
            },
            [&](const SchedulerGatedEvent& ev)
            {
                line = fmt::format(
                    "SchedulerGated for queryId {} priority {} gated_ns={} at {} ns\n",
                    ev.queryId,
                    ev.priority,
                    ev.gatedNs,
                    toNanoseconds(ev.timestamp));
            },
            [&](const StaircasePhaseStartEvent& ev)
            {
                line = fmt::format(
                    "StaircasePhaseStart for queryId {} priority {} phase_idx={} at {} ns\n",
                    ev.queryId,
                    ev.priority,
                    ev.phaseIdx,
                    toNanoseconds(ev.timestamp));
            }},
        event);

    {
        const std::lock_guard guard(coutMtx);
        std::cout << line << std::flush;
    }
}

}
