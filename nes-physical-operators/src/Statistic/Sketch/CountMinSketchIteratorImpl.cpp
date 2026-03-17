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

#include <Statistic/Sketch/CountMinSketchIteratorImpl.hpp>

namespace NES
{

constexpr static uint64_t ROW_FIELD_SIZE = sizeof(uint64_t);

CountMinSketchIteratorImpl::CountMinSketchIteratorImpl(
    const nautilus::val<int8_t*>& statisticMemArea, CountMinSketchProviderArguments& countMinProviderArguments)
    : StatisticProviderIteratorImpl(std::move(statisticMemArea)), countMinProviderArgs(std::move(countMinProviderArguments))
{
}

Record CountMinSketchIteratorImpl::operator*()
{
    Record record;
    record.write(countMinProviderArgs.rowFieldName, counterRow);
    record.write(countMinProviderArgs.columnFieldName, counterCol);
    record.write(
        countMinProviderArgs.counterFieldName, VarVal::readNonNullableVarValFromMemory(curCounter, countMinProviderArgs.counterDataType));
    return record;
}

StatisticProviderIteratorImpl& CountMinSketchIteratorImpl::operator++()
{
    counterRow += 1;
    if (counterRow >= numberOfRows)
    {
        counterRow = 0;
        counterCol += 1;
    }

    /// As the full 2-D array lies consecutively in memory, we can simply increment the curCounter
    curCounter += countMinProviderArgs.counterDataType.getSizeInBytesWithoutNull();

    return *this;
}

nautilus::val<bool> CountMinSketchIteratorImpl::operator==(const StatisticProviderIteratorImpl& other) const
{
    if (const auto otherCountMin = dynamic_cast<const CountMinSketchIteratorImpl*>(&other); otherCountMin != nullptr)
    {
        return numberOfColumns == otherCountMin->numberOfColumns and numberOfRows == otherCountMin->numberOfRows
            and counterCol == otherCountMin->counterCol and counterRow == otherCountMin->counterRow
            and curCounter == otherCountMin->curCounter and statisticMemArea == otherCountMin->statisticMemArea;
    }
    return false;
}

void CountMinSketchIteratorImpl::advanceToBegin()
{
    numberOfRows = readValueFromMemRef<uint64_t>(statisticMemArea + nautilus::val<uint64_t>(sizeOfTotalAreaSize + sizeOfMetaDataSize));
    numberOfColumns = readValueFromMemRef<uint64_t>(
        statisticMemArea + nautilus::val<uint64_t>{sizeOfTotalAreaSize + sizeOfMetaDataSize} + nautilus::val<uint64_t>{ROW_FIELD_SIZE});


    const auto metaDataSize = readValueFromMemRef<uint32_t>(statisticMemArea + sizeOfTotalAreaSize);
    counterCol = 0;
    counterRow = 0;
    curCounter = statisticMemArea + nautilus::val<uint64_t>{sizeOfTotalAreaSize + sizeOfMetaDataSize} + metaDataSize;
}

void CountMinSketchIteratorImpl::advanceToEnd()
{
    numberOfRows = readValueFromMemRef<uint64_t>(statisticMemArea + nautilus::val<uint8_t>(sizeOfTotalAreaSize + sizeOfMetaDataSize));
    numberOfColumns = readValueFromMemRef<uint64_t>(
        statisticMemArea + nautilus::val<uint64_t>{sizeOfTotalAreaSize + sizeOfMetaDataSize} + nautilus::val<uint64_t>{ROW_FIELD_SIZE});

    const auto metaDataSizeRef = statisticMemArea + sizeOfTotalAreaSize;
    const auto metaDataSize = readValueFromMemRef<uint32_t>(metaDataSizeRef);

    counterCol = numberOfColumns;
    /// The row counter is reset to zero when the column counter is increased.
    counterRow = 0;
    curCounter = statisticMemArea + nautilus::val<uint64_t>(sizeOfTotalAreaSize + sizeOfMetaDataSize) + metaDataSize
        + (countMinProviderArgs.counterDataType.getSizeInBytesWithoutNull() * numberOfColumns * numberOfRows);
}

}
