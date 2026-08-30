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

#include <mutex>
#include <BackpressureStatisticsListener.hpp>

namespace NES
{

/// Stdout emitter that turns BackpressureEvents into one log line each, mirroring the format of the
/// existing throughput / latency listeners so the bench script's regex-based parser fits.
///
/// Line formats (terminated with newline + flush):
///   BufferSent for queryId QueryId(local=<UUID>, distributed=<NAME>) priority <HIGH|LOW> tuples=<N> at <ns> ns
///   BufferIngest for queryId QueryId(local=<UUID>, distributed=<NAME>) priority <HIGH|LOW> tuples=<N> at <ns> ns
///   BackpressureApplied for queryId QueryId(local=<UUID>, distributed=<NAME>) priority <HIGH|LOW> at <ns> ns
///   BackpressureReleased for queryId QueryId(local=<UUID>, distributed=<NAME>) priority <HIGH|LOW> at <ns> ns
class BackpressureStatisticStdoutEmitter final : public BackpressureStatisticListener
{
public:
    void onEvent(BackpressureEvent event) override;

private:
    /// Serializes writes to std::cout so concurrent worker threads don't interleave bytes within one line.
    /// std::cout is not guaranteed atomic at the line level even with buffering disabled.
    std::mutex coutMtx;
};

}
