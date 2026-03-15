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
#include <LatencyListener.hpp>

#include <iostream>
#include <queue>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>
#include <Thread.hpp>

namespace NES
{
namespace
{

Timestamp convertToTimeStamp(const ChronoClock::time_point& timePoint)
{
    const unsigned long milliSecondsSinceEpoch
        = std::chrono::time_point_cast<std::chrono::milliseconds>(timePoint).time_since_epoch().count();
    INVARIANT(milliSecondsSinceEpoch > 0, "milliSecondsSinceEpoch should be larger than 0 but are {}", milliSecondsSinceEpoch);
    return Timestamp(milliSecondsSinceEpoch);
}

struct TaskIntermediateStore
{
    TaskIntermediateStore(QueryId queryId, const uint64_t numberOfTuples, ChronoClock::time_point startTimePoint)
        : queryId(queryId), numberOfTuples(numberOfTuples), startTimePoint(startTimePoint)
    {
    }

    explicit TaskIntermediateStore() : queryId(INVALID_QUERY_ID), numberOfTuples(0), startTimePoint(std::chrono::system_clock::now()) { }

    QueryId queryId;
    uint64_t numberOfTuples;
    ChronoClock::time_point startTimePoint;
};

struct TimestampAndLatencies
{
    explicit TimestampAndLatencies() : firstTimePoint(std::chrono::system_clock::now()) { }

    ChronoClock::time_point firstTimePoint;
    std::vector<std::chrono::duration<double>> latencies;
};

void threadRoutine(
    const std::stop_token& token,
    folly::Synchronized<std::queue<Event>>& events,
    const std::function<void(const LatencyListener::CallBackParams&)>& callBack,
    const uint64_t numberOfTasks)
{
    Thread::setThreadName("LatencyCalculator");

    std::unordered_map<TaskId, TaskIntermediateStore> intermediateStore;
    std::unordered_map<QueryId, TimestampAndLatencies> averageLatency;

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
                    /// We want to measure the latency from the ingestion timestamp to the time the task gets picked up
                    /// If this task did not belong to a formatting task, we ignore it and return
                    if (not taskEmit.formattingTask or taskEmit.fromPipeline == taskEmit.toPipeline)
                    {
                        return;
                    }

                    intermediateStore.insert(
                        {taskEmit.taskId, TaskIntermediateStore{taskEmit.queryId, taskEmit.numberOfTuples, taskEmit.timestamp}});
                },
                [&](const TaskExecutionComplete& taskExecutionCompleted)
                {
                    const auto taskId = taskExecutionCompleted.taskId;
                    if (const auto intermediateStoredTask = intermediateStore.find(taskId);
                        intermediateStoredTask != intermediateStore.end())
                    {
                        const auto latency = taskExecutionCompleted.timestamp - intermediateStoredTask->second.startTimePoint;
                        auto& [firstTimestamp, latencies] = averageLatency[taskExecutionCompleted.queryId];
                        latencies.emplace_back(latency);
                        if (latencies.size() == 1)
                        {
                            firstTimestamp = intermediateStoredTask->second.startTimePoint;
                        }

                        /// If this is the first tasks that gets completed
                        firstTimestamp = std::min(firstTimestamp, intermediateStoredTask->second.startTimePoint);

                        /// Once we have seen enough number of tasks, we call the callBack with the params
                        if (latencies.size() >= numberOfTasks)
                        {
                            const LatencyListener::CallBackParams callBackParams{
                                convertToTimeStamp(firstTimestamp),
                                convertToTimeStamp(taskExecutionCompleted.timestamp),
                                taskExecutionCompleted.queryId,
                                latencies.size(),
                                latency};
                            callBack(callBackParams);
                            averageLatency.erase(taskExecutionCompleted.queryId);
                        }
                        intermediateStore.erase(intermediateStoredTask);
                    }
                },
                [](auto) {}},
            event);
    }
}

}

void LatencyListener::onEvent(Event event)
{
    std::visit(
        Overloaded{
            [&](const TaskEmit& taskEmit) { events.wlock()->emplace(taskEmit); },
            [&](const TaskExecutionComplete& taskExecutionCompleted) { events.wlock()->emplace(taskExecutionCompleted); },
            [](auto) {}},
        event);
}

LatencyListener::~LatencyListener()
{
    /// We wait until the queue is empty or for 30 seconds
    const std::chrono::seconds timeout{30};
    const auto endTime = std::chrono::system_clock::now() + timeout;
    while (true)
    {
        /// Check if the queue is empty
        if (events.rlock()->empty())
        {
            return;
        }

        /// Check if the timeout has been reached
        if (std::chrono::system_clock::now() >= endTime)
        {
            std::cout << fmt::format(
                "Queue in LatencyListener still contains {} elements but could not finish in {}.", events.rlock()->size(), timeout)
                      << std::endl;
            NES_WARNING("Queue in LatencyListener still contains {} elements but could not finish in {}.", events.rlock()->size(), timeout);
        }
    }
}

LatencyListener::LatencyListener(const std::function<void(const CallBackParams&)>& callBack, const uint64_t numberOfTasks)
    : callBack(callBack)
    , calculateThread([this, numberOfTasks](const std::stop_token& stopToken)
                      { threadRoutine(stopToken, events, this->callBack, numberOfTasks); })
{
}

}
