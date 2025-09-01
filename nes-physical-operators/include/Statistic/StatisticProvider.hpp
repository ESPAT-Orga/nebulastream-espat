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

#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <Statistic.hpp>
#include <val.hpp>

namespace NES
{

/// Forward declaration to use it in StatisticProvider::VariantIterator
class StatisticProviderIteratorImpl;

/// @brief Abstract struct so that we can pass arguments to the underlying StatisticProviderIteratorImpl
struct StatisticProviderArguments
{
    bool operator==(const StatisticProviderArguments &) const
    {
        return true;
    }

    virtual ~StatisticProviderArguments() = default;
    virtual std::unique_ptr<StatisticProviderArguments> clone() = 0;
};

/// @brief This class provides an interface to the underlying implementation to operate/iterate over a statistic.
/// To add a new provider implementation, please override the @class StatisticProviderIteratorImpl
/// For now, we only support read operations in an interator fashion, as we build the statistic via window aggregations.
class StatisticProvider final
{
public:
    /// @brief Implements the common iterator methods by calling them on the underlying variant
    class StatisticProviderIterator final
    {
        std::unique_ptr<StatisticProviderIteratorImpl> iteratorImpl;

    public:
        virtual ~StatisticProviderIterator() = default;
        StatisticProviderIterator(StatisticProviderIterator&& statisticProviderIterator) noexcept;
        explicit StatisticProviderIterator(std::unique_ptr<StatisticProviderIteratorImpl> iteratorImpl);
        Record operator*() const;
        StatisticProviderIterator& operator++();
        nautilus::val<bool> operator==(const StatisticProviderIterator& other) const;
        nautilus::val<bool> operator!=(const StatisticProviderIterator& other) const;

    protected:
        friend class StatisticProvider;
        /// Gets called after creation but before the first use
        virtual void advanceToBegin() const;
        virtual void advanceToEnd() const;
    };

    StatisticProvider(const Statistic::StatisticType statisticType, std::unique_ptr<StatisticProviderArguments> statisticProviderArguments);
    StatisticProvider(StatisticProvider&& other) noexcept;
    StatisticProvider& operator=(StatisticProvider&& other) noexcept;
    StatisticProvider(const StatisticProvider& other) noexcept;
    StatisticProvider& operator=(const StatisticProvider& other) noexcept;

    ~StatisticProvider() = default;

    [[nodiscard]] StatisticProviderIterator begin() const;
    [[nodiscard]] StatisticProviderIterator end() const;

private:
    Statistic::StatisticType statisticType;
    std::unique_ptr<StatisticProviderArguments> statisticProviderArguments;
};

/// @brief This class provides an interface on operating statistics. The actual implementation heavily depends on
/// the actual underlying statistics implementation, e.g., samples, counter, histogram, etc.
/// The physical layout of the synopsis ref is the following. As we store the synopsis via var sized data, we reuse the 32-bit size field
/// of the var sized data.
/// | --- Total Size Of Memory Area --- | --- Meta-Data Size --- | --- Meta-Data ---     | --- Statistics Area ---    |
/// | ----------- 32bit ----------- --- | ------- 32bit -------  | --- metaDataSize ---  | --- totalSizeOfMemArea - metaDataSize ---  |
class StatisticProviderIteratorImpl
{
public:
    explicit StatisticProviderIteratorImpl(nautilus::val<int8_t*> statisticData);
    virtual ~StatisticProviderIteratorImpl() = default;

    /// Methods for operating in a reading manner across one statistic
    virtual Record operator*() = 0;
    virtual StatisticProviderIteratorImpl& operator++() = 0;
    virtual nautilus::val<bool> operator==(const StatisticProviderIteratorImpl& other) const = 0;

    virtual nautilus::val<bool> operator!=(const StatisticProviderIteratorImpl& other) const { return not(*this == other); }

protected:
    friend class StatisticProvider::StatisticProviderIterator;
    /// Methods for setting/advancing
    virtual void advanceToBegin() = 0;
    virtual void advanceToEnd() = 0;

    nautilus::val<uint32_t> sizeOfTotalAreaSize = 4;
    nautilus::val<uint32_t> sizeOfMetaDataSize = 4;
    nautilus::val<int8_t*> statisticMemArea;
};

}
