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
#include <Operators/Statistic/LogicalStatisticFields.hpp>
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
    const uint64_t numberOfSeenTuples,
    const int8_t* data,
    const uint32_t statisticDataSize)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");
    PRECONDITION(data != nullptr, "statistic data pointer should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    const auto statisticStore = opHandler->getStatisticStore();

    std::vector<int8_t> statisticData(statisticDataSize);
    std::memcpy(statisticData.data(), data, statisticDataSize);

    const Statistic statistic{
        hash,
        type,
        Windowing::TimeMeasure(startTs.getRawValue()),
        Windowing::TimeMeasure(endTs.getRawValue()),
        numberOfSeenTuples,
        statisticData.data(),
        statisticData.size()};


    statisticStore->insertStatistic(hash, statistic);
}

StatisticStoreWriter::StatisticStoreWriter(
    const OperatorHandlerId operatorHandlerId,
    const Statistic::StatisticHash statisticHash,
    const Statistic::StatisticType statisticType,
    const LogicalStatisticFields& inputLogicalStatisticFields,
    const LogicalStatisticFields& outputLogicalStatisticFields)
    : operatorHandlerId(operatorHandlerId)
    , statisticHash(statisticHash)
    , statisticType(statisticType)
    , inputStatisticStartTsFieldName(inputLogicalStatisticFields.statisticStartTsField.name)
    , inputStatisticEndTsFieldName(inputLogicalStatisticFields.statisticEndTsField.name)
    , inputStatisticDataFieldName(inputLogicalStatisticFields.statisticDataField.name)
    , inputStatisticNumberOfSeenTuplesFieldName(inputLogicalStatisticFields.statisticNumberOfSeenTuplesField.name)
    , outputStatisticStartTsFieldName(outputLogicalStatisticFields.statisticStartTsField.name)
    , outputStatisticEndTsFieldName(outputLogicalStatisticFields.statisticEndTsField.name)
    , outputStatisticHashFieldName(outputLogicalStatisticFields.statisticHashField.name)
    , outputStatisticNumberOfSeenTuplesFieldName(outputLogicalStatisticFields.statisticNumberOfSeenTuplesField.name)
{
}

void StatisticStoreWriter::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Insert statistic into store
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const nautilus::val<Statistic::StatisticHash> statisticHashVal{statisticHash};
    const nautilus::val<Statistic::StatisticType> statisticTypeVal{statisticType};
    const nautilus::val<Timestamp> startTs{record.read(inputStatisticStartTsFieldName).cast<nautilus::val<Timestamp::Underlying>>()};
    const nautilus::val<Timestamp> endTs{record.read(inputStatisticEndTsFieldName).cast<nautilus::val<Timestamp::Underlying>>()};
    const auto statisticData = record.read(inputStatisticDataFieldName).cast<VariableSizedData>().getReference();
    const auto totalSizeOfStatisticData = record.read(inputStatisticDataFieldName).cast<VariableSizedData>().getTotalSize();
    const auto numberOfSeenTuples = record.read(inputStatisticNumberOfSeenTuplesFieldName).cast<nautilus::val<uint64_t>>();
    invoke(
        insertStatisticIntoStoreProxy,
        operatorHandlerMemRef,
        statisticHashVal,
        statisticTypeVal,
        startTs,
        endTs,
        numberOfSeenTuples,
        statisticData,
        totalSizeOfStatisticData);

    /// Passing the startts, endTs and statistic hash to the next operator
    Record newRecord;
    newRecord.write(outputStatisticStartTsFieldName, startTs.convertToValue());
    newRecord.write(outputStatisticEndTsFieldName, endTs.convertToValue());
    newRecord.write(outputStatisticHashFieldName, statisticHashVal);
    newRecord.write(outputStatisticNumberOfSeenTuplesFieldName, numberOfSeenTuples);
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
