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

#include <atomic>
#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <SendingStrategy/NetworkSinkSendingStrategy.hpp>
#include <Priority.hpp>
#include <QueryId.hpp>

namespace NES
{

/// Reactive priority-based gating:
///   * HIGH-priority queries always send.
///   * LOW-priority queries are blocked while at least one HIGH-priority channel is currently
///     experiencing backpressure (i.e. between onBackpressureApplied and the matching onBackpressureReleased).
///
/// Implementation notes:
///   - register/deregister and per-id priority lookups use a shared_mutex; the hot-path maySend() acquires
///     a shared lock plus a single atomic load.
///   - LOW-priority backpressure events are intentionally ignored: a LOW query under load is the expected
///     outcome of the strategy itself, not a signal that should propagate.
class AdaptiveDifferentPrioStrategy final : public NetworkSinkSendingStrategy
{
public:
    void registerChannel(QueryId queryId, Priority priority) override;
    void deregisterChannel(QueryId queryId) override;
    [[nodiscard]] bool maySend(QueryId queryId) const override;
    void onBackpressureApplied(QueryId queryId) override;
    void onBackpressureReleased(QueryId queryId) override;
    void onBufferSent(QueryId queryId, uint64_t numTuples) override;

private:
    struct ChannelState
    {
        Priority priority;
        bool inBackpressure;
    };

    mutable std::shared_mutex mutex;
    std::unordered_map<QueryId, ChannelState> channels;
    std::atomic<uint32_t> highChannelsUnderBackpressure{0};
};

}
