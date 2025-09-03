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

#include <Statistic/Histogram/EquiWidthHistogramIteratorImpl.hpp>

namespace NES
{
EquiWidthHistogramIteratorImpl::EquiWidthHistogramIteratorImpl(
    const nautilus::val<int8_t*>& statisticMemArea, EquiWidthHistogramProviderArguments equiWidthHistogramArguments)
    : StatisticProviderIteratorImpl(std::move(statisticMemArea)), equiWidthHistogramArgs(std::move(equiWidthHistogramArguments))
{
}

Nautilus::Record EquiWidthHistogramIteratorImpl::operator*()
{
    /// Reading the current bin into VarVals
    nautilus::val<uint64_t> dataTypeStartEndVal{equiWidthHistogramArgs.dataTypeStartEnd.getSizeInBytes()};
    const auto binCounterMemRef = binMemRef + dataTypeStartEndVal;
    const auto binEndMemRef = binMemRef + dataTypeStartEndVal * 2;
    const auto binStart = VarVal::readVarValFromMemory(binMemRef, equiWidthHistogramArgs.dataTypeStartEnd.type);
    const auto binCounter = VarVal::readVarValFromMemory(binCounterMemRef, equiWidthHistogramArgs.dataTypeCounter.type);
    const auto binEnd = VarVal::readVarValFromMemory(binEndMemRef, equiWidthHistogramArgs.dataTypeStartEnd.type);

    /// Create a record out of the histogram bin
    Record record;
    record.write(equiWidthHistogramArgs.binStartFieldName, binStart);
    record.write(equiWidthHistogramArgs.binEndFieldName, binEnd);
    record.write(equiWidthHistogramArgs.binCounterFieldName, binCounter);
    return record;
}

StatisticProviderIteratorImpl& EquiWidthHistogramIteratorImpl::operator++()
{
    binMemRef += equiWidthHistogramArgs.sizeOfDataType;
    return *this;
}

nautilus::val<bool> EquiWidthHistogramIteratorImpl::operator==(const StatisticProviderIteratorImpl& other) const
{
    if (const auto otherEquiWidthHistogram = dynamic_cast<const EquiWidthHistogramIteratorImpl*>(&other); otherEquiWidthHistogram != nullptr)
    {
        return binMemRef == otherEquiWidthHistogram->binMemRef and statisticMemArea == otherEquiWidthHistogram->statisticMemArea
            and numberOfBins == otherEquiWidthHistogram->numberOfBins and equiWidthHistogramArgs == otherEquiWidthHistogram->equiWidthHistogramArgs;
    }
    return false;
}

void EquiWidthHistogramIteratorImpl::advanceToBegin()
{
    const auto noTuplesMemRef = statisticMemArea + (sizeOfTotalAreaSize + sizeOfMetaDataSize);
    numberOfBins = NES::Util::readValueFromMemRef<uint64_t>(noTuplesMemRef);

    const auto metaDataSizeRef = statisticMemArea + sizeOfTotalAreaSize;
    const auto metaDataSize = Util::readValueFromMemRef<uint32_t>(metaDataSizeRef);
    binMemRef = statisticMemArea + sizeOfTotalAreaSize + sizeOfMetaDataSize + metaDataSize;
}

void EquiWidthHistogramIteratorImpl::advanceToEnd()
{
    const auto noTuplesMemRef = statisticMemArea + (sizeOfTotalAreaSize + sizeOfMetaDataSize);
    numberOfBins = NES::Util::readValueFromMemRef<uint64_t>(noTuplesMemRef);

    const auto metaDataSizeRef = statisticMemArea + sizeOfTotalAreaSize;
    const auto metaDataSize = Util::readValueFromMemRef<uint32_t>(metaDataSizeRef);
    binMemRef
        = statisticMemArea + sizeOfTotalAreaSize + sizeOfMetaDataSize + metaDataSize + numberOfBins * equiWidthHistogramArgs.sizeOfDataType;
}
}
