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
#include <SendingStrategy/NetworkSinkSendingStrategy.hpp>
#include <Priority.hpp>
#include <QueryId.hpp>

namespace NES
{

/// Bandwidth-aware weighted-priority strategy dispatcher.
///
/// Returns `SendVariant::Weighted` for every query. `NetworkSink::execute` interprets that
/// variant as "consult `BackpressureController::isScheduledToSend(buffer.size_bytes)` first; on
/// approval, fall through to the unconditional Rust `send_buffer`". The actual gate logic lives
/// in the worker-wide `AdaptiveSendingScheduler` (HTB-style per-class share with residual
/// redistribution + online capacity estimation).
///
/// This class is a thin marker — registration with the scheduler happens at query-plan
/// instantiation via `BackpressureController::registerWithScheduler`, independent of the
/// NetworkSinkSendingStrategy. All event hooks are no-ops; instrumentation continues to flow
/// through the existing `BackpressureStatisticListener` path.
class WeightedPriorityStrategy final : public NetworkSinkSendingStrategy
{
public:
    void registerChannel(QueryId, Priority) override { }

    void deregisterChannel(QueryId) override { }

    [[nodiscard]] SendVariant sendVariant(QueryId) const override { return SendVariant::Weighted; }

    void onBackpressureApplied(QueryId) override { }

    void onBackpressureReleased(QueryId) override { }

    void onBufferSent(QueryId, uint64_t) override { }
};

}
