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

/// Selectable strategies that decide whether a query may send via a NetworkSink.
/// Selected at worker startup via the configuration option `network_sink_sending_strategy`.
enum class NetworkSinkSendingStrategyType : uint8_t
{
    /// Every query may always send. No coordination across queries.
    ALWAYS_SEND,
    /// Per-worker HTB-style scheduler in C++ allocates a configurable share of the wire to each
    /// priority class (default HIGH=0.8 / LOW=0.2). Wire stays fully utilized via residual
    /// redistribution from empty classes; LOW always has a guaranteed liveness floor — no
    /// starvation under sustained HIGH oversubscription. Strict priority is the limit case
    /// at HIGH=1.0 / LOW=0.0 (no operator-facing strategy enum needed — just configure the
    /// weights). See AdaptiveSendingScheduler.
    WEIGHTED_PRIO
};

}
