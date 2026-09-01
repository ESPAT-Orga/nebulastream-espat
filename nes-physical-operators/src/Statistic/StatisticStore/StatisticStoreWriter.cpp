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

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <utility>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <StatisticTuple.hpp>
#include <Statistic/StatisticStore/StatisticStoreOperatorHandler.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Time/Timestamp.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val_enum.hpp>

namespace NES
{

/// Copies the payload out of the pipeline arena and into a StatisticTuple owned by the store. The copy is required:
/// the arena memory is recycled once the pipeline moves on, whereas the store outlives the query that wrote it.
void insertStatisticIntoStoreProxy(
    OperatorHandler* ptrOpHandler,
    const StatisticId statisticId,
    const StatisticType type,
    const Timestamp startTs,
    const Timestamp endTs,
    const uint64_t numberOfSeenTuples,
    const int8_t* data,
    const uint32_t statisticDataSize)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler should not be null!");
    PRECONDITION(data != nullptr, "statistic data pointer should not be null!");

    const auto* opHandler = dynamic_cast<StatisticStoreOperatorHandler*>(ptrOpHandler);
    PRECONDITION(opHandler != nullptr, "opHandler should be a StatisticStoreOperatorHandler!");
    const auto statisticStore = opHandler->getStatisticStore();

    auto statisticData = std::make_shared<std::byte[]>(statisticDataSize);
    std::memcpy(statisticData.get(), data, statisticDataSize);

    const StatisticTuple statistic{
        statisticId,
        type,
        Windowing::TimeMeasure(startTs.getRawValue()),
        Windowing::TimeMeasure(endTs.getRawValue()),
        numberOfSeenTuples,
        statisticData,
        statisticDataSize};

    statisticStore->insertStatistic(statisticId, statistic);
}

StatisticStoreWriter::StatisticStoreWriter(
    const OperatorHandlerId operatorHandlerId,
    const StatisticId statisticId,
    const StatisticType statisticType,
    Record::RecordFieldIdentifier dataFieldName,
    Record::RecordFieldIdentifier startTsFieldName,
    Record::RecordFieldIdentifier endTsFieldName,
    Record::RecordFieldIdentifier numberOfSeenTuplesFieldName,
    Record::RecordFieldIdentifier outputStatisticIdFieldName)
    : operatorHandlerId(operatorHandlerId)
    , statisticId(statisticId)
    , statisticType(statisticType)
    , dataFieldName(std::move(dataFieldName))
    , startTsFieldName(std::move(startTsFieldName))
    , endTsFieldName(std::move(endTsFieldName))
    , numberOfSeenTuplesFieldName(std::move(numberOfSeenTuplesFieldName))
    , outputStatisticIdFieldName(std::move(outputStatisticIdFieldName))
{
}

void StatisticStoreWriter::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const nautilus::val<StatisticId> statisticIdVal{statisticId};
    const nautilus::val<StatisticType> statisticTypeVal{statisticType};
    const nautilus::val<Timestamp> startTs{record.read(startTsFieldName).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};
    const nautilus::val<Timestamp> endTs{record.read(endTsFieldName).getRawValueAs<nautilus::val<Timestamp::Underlying>>()};
    const auto numberOfSeenTuples = record.read(numberOfSeenTuplesFieldName).getRawValueAs<nautilus::val<uint64_t>>();
    const auto statisticData = record.read(dataFieldName).getRawValueAs<VariableSizedData>();

    invoke(
        insertStatisticIntoStoreProxy,
        operatorHandlerMemRef,
        statisticIdVal,
        statisticTypeVal,
        startTs,
        endTs,
        numberOfSeenTuples,
        statisticData.getContent(),
        statisticData.getSize());

    /// Forward the record with STATISTICID added, so anything downstream of the aggregation still sees the window
    /// and can be told which statistic it belongs to.
    record.write(outputStatisticIdFieldName, statisticIdVal.convertToValue());
    executeChild(executionCtx, record);
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
