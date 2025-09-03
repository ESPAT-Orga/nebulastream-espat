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
    const nautilus::val<int8_t*>& statisticMemArea, EquiWidthHistogramProviderArguments& equiWidthHistogramArguments)
    : StatisticProviderIteratorImpl(std::move(statisticMemArea))
    , binStartFieldName(equiWidthHistogramArguments.binStartFieldName)
    , binEndFieldName(equiWidthHistogramArguments.binEndFieldName)
    , binCounterFieldName(equiWidthHistogramArguments.binCounterFieldName)
    , dataTypeCounter(equiWidthHistogramArguments.dataTypeCounter)
    , dataTypeStartEnd(equiWidthHistogramArguments.dataTypeStartEnd)
{
    sizeOfDataType = dataTypeCounter.getSizeInBytes() + dataTypeStartEnd.getSizeInBytes() * 2;
}

Nautilus::Record EquiWidthHistogramIteratorImpl::operator*() const
{
    /// Reading the current bin into VarVals
    nautilus::val<uint64_t> dataTypeStartEndVal{dataTypeStartEnd.getSizeInBytes()};
    const auto binCounterMemRef = binMemRef + dataTypeStartEndVal;
    const auto binEndMemRef = binMemRef + dataTypeStartEndVal * 2;
    const auto binStart = VarVal::readVarValFromMemory(binMemRef, dataTypeStartEnd.type);
    const auto binCounter = VarVal::readVarValFromMemory(binCounterMemRef, dataTypeCounter.type);
    const auto binEnd = VarVal::readVarValFromMemory(binEndMemRef, dataTypeStartEnd.type);

    /// Create a record out of the histogram bin
    Record record;
    record.write(binStartFieldName, binStart);
    record.write(binEndFieldName, binEnd);
    record.write(binCounterFieldName, binCounter);
    return record;
}

StatisticProviderIteratorImpl& EquiWidthHistogramIteratorImpl::operator++()
{
    binMemRef += sizeOfDataType;
    return *this;
}

nautilus::val<bool> EquiWidthHistogramIteratorImpl::operator==(const StatisticProviderIteratorImpl& other) const
{
    if (const auto otherReservoirSample = dynamic_cast<const EquiWidthHistogramIteratorImpl*>(&other); otherReservoirSample != nullptr)
    {
        return binMemRef == otherReservoirSample->binMemRef and statisticMemArea == otherReservoirSample->statisticMemArea
            and numberOfBins == otherReservoirSample->numberOfBins;
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
    binMemRef = statisticMemArea + sizeOfTotalAreaSize + sizeOfMetaDataSize + metaDataSize + numberOfBins * sizeOfDataType;
}
}
