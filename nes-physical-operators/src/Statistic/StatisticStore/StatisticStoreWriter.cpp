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
#include <Statistic/StatisticStore/StatisticStoreWriter.hpp>

#include <Nautilus/Interface/Record.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <ExecutionContext.hpp>
#include <val_enum.hpp>

namespace NES
{

void insertStatisticIntoStoreProxy(
    OperatorHandler* ptrOpHandler,
    const Statistic::StatisticHash hash,
    const Statistic::StatisticType type,
    const Timestamp startTs,
    const Timestamp endTs,
    int8_t* data)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");
    PRECONDITION(data != nullptr, "statistic data pointer should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    const auto statisticDataSize = *reinterpret_cast<uint32_t*>(data) + sizeof(uint32_t);
    std::vector<int8_t> statisticData(statisticDataSize);
    std::memcpy(statisticData.data(), data, statisticDataSize);

    const Statistic statistic{
        hash,
        type,
        Windowing::TimeMeasure(startTs.getRawValue()),
        Windowing::TimeMeasure(endTs.getRawValue()),
        statisticData.data(),
        statisticData.size()};

    statisticStore->insertStatistic(hash, statistic);
}

StatisticStoreWriter::StatisticStoreWriter(
    const uint64_t operatorHandlerId,
    const std::string_view statisticHashFieldName,
    const std::string_view statisticTypeFieldName,
    const std::string_view statisticStartTsFieldName,
    const std::string_view statisticEndTsFieldName,
    const std::string_view statisticDataFieldName)
    : operatorHandlerId(operatorHandlerId)
    , statisticHashFieldName(statisticHashFieldName)
    , statisticTypeFieldName(statisticTypeFieldName)
    , statisticStartTsFieldName(statisticStartTsFieldName)
    , statisticEndTsFieldName(statisticEndTsFieldName)
    , statisticDataFieldName(statisticDataFieldName)
{
}

void StatisticStoreWriter::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Insert statistic into store
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto statisticHash = record.read(statisticHashFieldName).cast<nautilus::val<Statistic::StatisticHash>>();
    const nautilus::val<Statistic::StatisticType> statisticType
        = record.read(statisticTypeFieldName).cast<nautilus::val<std::underlying_type_t<Statistic::StatisticType>>>();
    const nautilus::val<Timestamp> startTs{record.read(statisticStartTsFieldName).cast<nautilus::val<Timestamp::Underlying>>()};
    const nautilus::val<Timestamp> endTs{record.read(statisticEndTsFieldName).cast<nautilus::val<Timestamp::Underlying>>()};
    const auto statisticData = record.read(statisticDataFieldName).cast<VariableSizedData>().getReference();
    invoke(insertStatisticIntoStoreProxy, operatorHandlerMemRef, statisticHash, statisticType, startTs, endTs, statisticData);

    /// Passing the startts, endTs and statistic hash to the next operator
    Record newRecord;
    newRecord.write(statisticStartTsFieldName, startTs.convertToValue());
    newRecord.write(statisticEndTsFieldName, endTs.convertToValue());
    newRecord.write(statisticHashFieldName, statisticHash);
    executeChild(executionCtx, newRecord);
}

std::optional<PhysicalOperator> StatisticStoreWriter::getChild() const
{
    return child;
}

void StatisticStoreWriter::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
