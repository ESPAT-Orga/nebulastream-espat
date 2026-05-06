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

#include <SendingStrategy/NetworkSinkSendingStrategyFactory.hpp>

#include <memory>
#include <utility>
#include <NetworkSinkSendingStrategyType.hpp>
#include <SendingStrategy/AdaptiveDifferentPrioStrategy.hpp>
#include <SendingStrategy/AlwaysSendStrategy.hpp>
#include <SendingStrategy/NetworkSinkSendingStrategy.hpp>

namespace NES
{

std::shared_ptr<NetworkSinkSendingStrategy> createNetworkSinkSendingStrategy(NetworkSinkSendingStrategyType type)
{
    switch (type)
    {
        case NetworkSinkSendingStrategyType::ALWAYS_SEND:
            return std::make_shared<AlwaysSendStrategy>();
        case NetworkSinkSendingStrategyType::ADAPTIVE_DIFFERENT_PRIO:
            return std::make_shared<AdaptiveDifferentPrioStrategy>();
    }
    std::unreachable();
}

}
