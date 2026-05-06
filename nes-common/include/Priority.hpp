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
#include <ostream>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// Per-query priority used by NetworkSinkSendingStrategy implementations to decide whether a query may send.
/// HIGH-priority queries are always allowed to send. LOW-priority queries may be throttled by adaptive strategies
/// when HIGH-priority queries are experiencing backpressure.
enum class Priority : uint8_t
{
    HIGH,
    LOW
};

inline std::ostream& operator<<(std::ostream& os, Priority priority)
{
    switch (priority)
    {
        case Priority::HIGH:
            return os << "HIGH";
        case Priority::LOW:
            return os << "LOW";
    }
    return os << "UNKNOWN";
}

}

FMT_OSTREAM(NES::Priority);
