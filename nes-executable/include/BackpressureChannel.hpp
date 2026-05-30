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

#include <memory>
#include <stop_token>
#include <utility>
#include <vector>

struct Channel;
class BackpressureListener;
class BackpressureController;

/// This is the entrypoint to a backpressure channel. It creates a pair of connected Backpressure Controller and BackpressureListener.
/// A Backpressure Controller controls the Backpressure, and a BackpressureListener only allows further progress if there is no backpressure.
/// In NebulaStream a Backpressure Controller is owned by exactly one sink, which controls all the BackpressureListener of all sources within the same query plan.
/// Currently, the Backpressure channel enforces the invariant that sinks always outlive sources. Thus, if a Backpressure Controller is destroyed, all
/// connected BackpressureListeners that are still alive and in use will report an assertion failure.
std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();

/// A Backpressure Controller is the exclusive controller of a backpressure channel. It allows the user to apply and release backpressure, which blocks
/// or unblocks all connected Ingestions.
class BackpressureController
{
    explicit BackpressureController(std::shared_ptr<Channel> channel);

    std::shared_ptr<Channel> channel;
    friend std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();

public:
    ~BackpressureController();

    /// Currently, a Backpressure Controller represents unique ownership over the backpressure channel, thus copying is not enabled.
    BackpressureController(const BackpressureController& other) = delete;
    BackpressureController& operator=(const BackpressureController& other) = delete;

    /// Default moves leaves channel in an empty state which prevents unintended destruction of the underlying channel
    BackpressureController(BackpressureController&& other) noexcept = default;
    BackpressureController& operator=(BackpressureController&& other) noexcept = default;

    bool applyPressure();
    bool releasePressure();
};

/// Listener of one or more backpressure channels, used by sources. Before initiating a read of a new buffer,
/// the source can check whether any controller has applied backpressure by calling `wait`. The thread blocks
/// on the call if backpressure has been applied on any of the underlying channels, until pressure is released
/// (or the stop token is signaled). When a query plan has multiple sinks, each sink owns its own controller;
/// the listener returned by `createBackpressureChannel` is composed via `merge` so sources only need to hold
/// a single listener that aggregates all sinks' backpressure signals.
class BackpressureListener
{
    explicit BackpressureListener(std::shared_ptr<Channel> channel) : channels{std::move(channel)} { }

    friend std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();
    std::vector<std::shared_ptr<Channel>> channels;

public:
    void wait(const std::stop_token& stopToken) const;

    /// Append `other`'s channels into this listener. After merge, `wait()` waits on every channel; if any
    /// controller applies pressure, the source blocks until that controller releases.
    void merge(BackpressureListener other);
};
