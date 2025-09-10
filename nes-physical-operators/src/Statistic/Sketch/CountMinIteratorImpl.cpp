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

#include <Statistic/Sketch/CountMinIteratorImpl.hpp>

namespace NES
{

CountMinIteratorImpl::CountMinIteratorImpl(
    const nautilus::val<int8_t*>& statisticMemArea, CountMinProviderArguments& countMinProviderArguments)
    : StatisticProviderIteratorImpl(std::move(statisticMemArea)), countMinProviderArgs(std::move(countMinProviderArguments))
{
}

Record CountMinIteratorImpl::operator*()
{
    Record record;
    record.write(countMinProviderArgs.rowFieldName, counterRow);
    record.write(countMinProviderArgs.columnFieldName, counterCol);
    record.write(
        countMinProviderArgs.counterFieldName, VarVal::readVarValFromMemory(curCounter, countMinProviderArgs.counterDataType.type));
    return record;
}

StatisticProviderIteratorImpl& CountMinIteratorImpl::operator++()
{
    /// Continuing to the next row, if we are at the end of the current on
    if (counterCol >= numberOfColumns)
    {
        counterCol = 0;
        counterRow += 1;
    }

    /// As the full 2-D array lies consequetively in memory, we can simply increment the curCounter
    curCounter += countMinProviderArgs.counterDataType.getSizeInBytes();

    return *this;
}

nautilus::val<bool> CountMinIteratorImpl::operator==(const StatisticProviderIteratorImpl& other) const
{
    if (const auto otherCountMin = dynamic_cast<const CountMinIteratorImpl*>(&other); otherCountMin != nullptr)
    {
        return countMinProviderArgs.counterDataType == otherCountMin->countMinProviderArgs.counterDataType
            and numberOfColumns == otherCountMin->numberOfColumns and numberOfRows == otherCountMin->numberOfRows
            and counterCol == otherCountMin->counterCol and counterRow == otherCountMin->counterRow
            and curCounter == otherCountMin->curCounter and statisticMemArea == otherCountMin->statisticMemArea;
    }
    return false;
}

void CountMinIteratorImpl::advanceToBegin()
{
    numberOfRows = readValueFromMemRef<uint64_t>(statisticMemArea + sizeOfTotalAreaSize + sizeOfMetaDataSize);
    numberOfColumns
        = readValueFromMemRef<uint64_t>(statisticMemArea + sizeOfTotalAreaSize + sizeOfMetaDataSize + nautilus::val<uint64_t>(8));


    const auto metaDataSize = readValueFromMemRef<uint32_t>(statisticMemArea + sizeOfTotalAreaSize);
    counterCol = 0;
    counterRow = 0;
    curCounter = statisticMemArea + sizeOfTotalAreaSize + sizeOfMetaDataSize + metaDataSize;
}

void CountMinIteratorImpl::advanceToEnd()
{
    numberOfRows = readValueFromMemRef<uint64_t>(statisticMemArea + sizeOfTotalAreaSize + sizeOfMetaDataSize);
    numberOfColumns
        = readValueFromMemRef<uint64_t>(statisticMemArea + sizeOfTotalAreaSize + sizeOfMetaDataSize + nautilus::val<uint64_t>(8));

    counterCol = numberOfColumns;
    counterRow = numberOfRows;
}

}
