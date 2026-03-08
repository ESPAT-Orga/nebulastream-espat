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
#include <map>
#include <unordered_map>
#include <utility>
#include <SliceStore/SliceAssigner.hpp>
#include <Util/Overloaded.hpp>
#include <Thread.hpp>
#include <ThroughputListener.hpp>

namespace NES
{

namespace
{

Timestamp convertToTimeStamp(const ChronoClock::time_point timePoint)
{
    const unsigned long milliSecondsSinceEpoch
        = std::chrono::time_point_cast<std::chrono::milliseconds>(timePoint).time_since_epoch().count();
    INVARIANT(milliSecondsSinceEpoch > 0, "milliSecondsSinceEpoch should be larger than 0 but are {}", milliSecondsSinceEpoch);
    return Timestamp(milliSecondsSinceEpoch);
}

void threadRoutine(
    const std::stop_token& token,
    const Timestamp::Underlying timeIntervalInMilliSeconds,
    folly::Synchronized<std::queue<Event>>& events,
    const std::function<void(const ThroughputListener::CallBackParams&)>& callBack)
{
    PRECONDITION(callBack != nullptr, "Call Back is null");

    Thread::setThreadName("ThroughputCalculator");

    /// All variables necessary for performing a simple tumbling window average aggregation per query id
    const SliceAssigner sliceAssigner(timeIntervalInMilliSeconds, timeIntervalInMilliSeconds);

    struct ThroughputWindow
    {
        explicit ThroughputWindow()
            : startTime(Timestamp::INVALID_VALUE), endTime(Timestamp::INVALID_VALUE), tuplesProcessed(0)
        {
        }

        Timestamp startTime;
        Timestamp endTime;
        uint64_t tuplesProcessed;

        bool operator<(const ThroughputWindow& other) const { return endTime < other.endTime; }
    };

    /// We need to have for each query id windows that store the number of tuples processed in one.
    /// For faster access, we store the window with its end time as the key in a hash map.
    std::unordered_map<QueryId, std::map<Timestamp, ThroughputWindow>> queryIdToThroughputWindowMap;

    while (!token.stop_requested())
    {
        if (events.rlock()->empty())
        {
            continue;
        }

        /// Using lambda invocation to gain the possibility of a return
        /// As we only have one thread removing items from the queue, we are sure that we will retrieve a new item here
        auto event = [&events]()
        {
            const auto lockedEvents = events.wlock();
            const auto newEvent = lockedEvents->front();
            lockedEvents->pop();
            return newEvent;
        }();

        std::visit(
            Overloaded{
                [&](const TaskEmit& taskEmit)
                {
                    /// We define the throughput to be the performance of the formatting steps, i.e., the throughput of emitting work from the formatting
                    /// If this task did not belong to a formatting task, we ignore it and return
                    if (not taskEmit.formattingTask or taskEmit.fromPipeline == taskEmit.toPipeline)
                    {
                        return;
                    }
                    const auto numberOfTuples = taskEmit.numberOfTuples;
                    // const auto bytes = taskEmit.bytesInTupleBuffer;
                    const auto queryId = taskEmit.queryId;
                    const auto endTime = convertToTimeStamp(taskEmit.timestamp);
                    const auto windowStart = sliceAssigner.getSliceStartTs(endTime);
                    const auto windowEnd = sliceAssigner.getSliceEndTs(endTime);
                    queryIdToThroughputWindowMap[queryId][windowEnd].tuplesProcessed += numberOfTuples;
                    queryIdToThroughputWindowMap[queryId][windowEnd].startTime = windowStart;
                    queryIdToThroughputWindowMap[queryId][windowEnd].endTime = windowEnd;
                    // std::cout << "Got taskEmit " << taskEmit.taskId << " with " << numberOfTuples << " tuples, " << bytes << " bytes and endTime " << endTime  << " for " << windowStart << " to " << windowEnd << std::endl;


                    /// Now we need to check if we can emit / calculate a throughput. We assume that taskStopEvent.timestamp is increasing
                    for (auto& [queryId, endTimeAndThroughputWindow] : queryIdToThroughputWindowMap)
                    {
                        /// We need at least two windows per query to calculate a throughput
                        if (endTimeAndThroughputWindow.size() < 2)
                        {
                            continue;
                        }
                        auto it = endTimeAndThroughputWindow.begin();
                        while (it != std::prev(endTimeAndThroughputWindow.end()))
                        {
                            const auto& curWindowEnd = it->first;
                            const auto& [startTime, endTime, tuplesProcessed] = it->second;
                            if (curWindowEnd + timeIntervalInMilliSeconds >= windowEnd)
                            {
                                /// As the windows are sorted by their end time, we can break here
                                break;
                            }

                            /// Calculating the throughput over this window and letting the callback know that a new throughput has been calculated
                            auto nextEndTime = std::next(it)->first;
                            const auto durationInMilliseconds = (nextEndTime - endTime).getRawValue();
                            const auto throughputInTuplesPerSec = tuplesProcessed / (durationInMilliseconds / 1000.0);
                            // std::cout << "nextEndTime " << nextEndTime << std::endl;
                            // std::cout << "endTime " << endTime << std::endl;
                            // std::cout << "bytesProcessed " << bytesProcessed << std::endl;
                            // std::cout << "tuplesProcessed " << tuplesProcessed << std::endl;
                            // std::cout << "throughputInBytesPerSec " << throughputInBytesPerSec << std::endl;
                            // std::cout << "throughputInTuplesPerSec " << throughputInTuplesPerSec << std::endl;
                            const ThroughputListener::CallBackParams callbackParams
                                = {.queryId = queryId,
                                   .windowStart = startTime,
                                   .windowEnd = endTime,
                                   .throughputInTuplesPerSec = throughputInTuplesPerSec};
                            callBack(callbackParams);

                            /// Removing the window, as we do not need it anymore
                            it = endTimeAndThroughputWindow.erase(it);
                        }
                    }
                },
                [](auto) {}},
            event);
    }
}
}

ThroughputListener::~ThroughputListener()
{
    /// We wait until the queue is empty or for 30 seconds
    const std::chrono::seconds timeout{30};
    const auto endTime = std::chrono::high_resolution_clock::now() + timeout;
    while (true)
    {
        /// Check if the queue is empty
        if (events.rlock()->empty())
        {
            return;
        }

        /// Check if the timeout has been reached
        if (std::chrono::high_resolution_clock::now() >= endTime)
        {
            std::cout << fmt::format(
                "Queue in ThroughputListener still contains {} elements but could not finish in {}.", events.rlock()->size(), timeout)
                      << std::endl;
            NES_WARNING(
                "Queue in ThroughputListener still contains {} elements but could not finish in {}.", events.rlock()->size(), timeout);
        }
    }
}

void ThroughputListener::onEvent(Event event)
{
    std::visit(Overloaded{[&](const TaskEmit& taskEmit) { events.wlock()->emplace(taskEmit); }, [](auto) {}}, event);
}

ThroughputListener::ThroughputListener(
    const Timestamp::Underlying timeIntervalInMilliSeconds, const std::function<void(const CallBackParams&)>& callBack)
    : timeIntervalInMilliSeconds(timeIntervalInMilliSeconds)
    , callBack(callBack)
    , calculateThread([this](const std::stop_token& stopToken)
                      { threadRoutine(stopToken, this->timeIntervalInMilliSeconds, events, this->callBack); })

{
}
}
