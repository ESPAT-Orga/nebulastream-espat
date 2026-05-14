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

#include <cstdint>

namespace NES
{

/// Selectable capacity-estimation modes for the AdaptiveSendingScheduler.
/// The enum lives in nes-common (instead of with the CapacityEstimator class itself in
/// nes-executable) because WorkerNetworkConfiguration in nes-network needs to expose it as an
/// EnumOption — and nes-network is below nes-executable in the dependency stack.
enum class CapacityEstimatorMode : uint8_t
{
    /// Track observed throughput via an exponentially weighted moving average (EMA). Adapts to
    /// real wire conditions, but when the source can't keep the wire full the EMA tracks the
    /// lower delivered rate, so the estimate drifts down toward that source-limited rate; the cap
    /// is then briefly under-estimated for a few ticks once the source speeds up again.
    EMA,
    /// Use a fixed, configured capacity. Suitable when the wire capacity is known a priori
    /// (e.g., a tc-throttled experimental setup); sidesteps EMA-erosion artifacts.
    FIXED,
};

}
