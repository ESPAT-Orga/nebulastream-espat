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

/// Trivial sending strategy: maySend() always returns true; event hooks are no-ops.
class AlwaysSendStrategy final : public NetworkSinkSendingStrategy
{
public:
    void registerChannel(QueryId, Priority) override { }
    void deregisterChannel(QueryId) override { }
    [[nodiscard]] bool maySend(QueryId) const override { return true; }
    void onBackpressureApplied(QueryId) override { }
    void onBackpressureReleased(QueryId) override { }
    void onBufferSent(QueryId, uint64_t) override { }
};

}
