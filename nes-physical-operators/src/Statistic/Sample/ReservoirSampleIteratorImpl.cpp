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

#include <Statistic/Sample/ReservoirSampleIteratorImpl.hpp>

namespace NES
{
ReservoirSampleIteratorImpl::ReservoirSampleIteratorImpl(
    const nautilus::val<int8_t*>& statisticMemArea, ReservoirSampleProviderArguments& reservoirSampleArguments)
    : StatisticProviderIteratorImpl(std::move(statisticMemArea)), memoryLayout(std::move(reservoirSampleArguments.memoryLayout))
{
}

Nautilus::Record ReservoirSampleIteratorImpl::operator*() const
{
    INVARIANT(recordPos <= sampleSize, "Reading sample item that does not exist");

    /// As we can not assume that the statisitc data is backed by a tuple buffer, we need to manually take care
    /// of variable sized data. For fixed data, we can use the TupleBufferMemoryProvider::read()
    Record record;
    for (nautilus::static_val<uint64_t> fieldIndex = 0; fieldIndex < memoryLayout->getSchema().getNumberOfFields(); ++fieldIndex)
    {
        auto fieldOffset = memoryLayout->getFieldOffset(recordPos, fieldIndex);
        auto fieldAddress = sampleData + fieldOffset;
        const auto fieldName = memoryLayout->getSchema().getFieldAt(fieldIndex).name;
        const auto physicalType = memoryLayout->getPhysicalType(fieldIndex).type;
        if (physicalType == DataType::Type::VARSIZED)
        {
            VariableSizedData varSizedData{fieldAddress};
            record.write(fieldName, varSizedData);
        }
        else
        {
            const auto varVal = VarVal::readVarValFromMemory(fieldAddress, physicalType);
            record.write(fieldName, varVal);
        }
    }

    return record;
}

StatisticProviderIteratorImpl& ReservoirSampleIteratorImpl::operator++()
{
    recordPos += 1;
    return *this;
}

nautilus::val<bool> ReservoirSampleIteratorImpl::operator==(const StatisticProviderIteratorImpl& other) const
{
    if (const auto otherReservoirSample = dynamic_cast<const ReservoirSampleIteratorImpl*>(&other); otherReservoirSample != nullptr)
    {
        return recordPos == otherReservoirSample->recordPos and statisticMemArea == otherReservoirSample->statisticMemArea
            and sampleSize == otherReservoirSample->sampleSize;
    }
    return false;
}

void ReservoirSampleIteratorImpl::advanceToBegin()
{
    const auto noTuplesMemRef = statisticMemArea + (sizeOfTotalAreaSize + sizeOfMetaDataSize);
    sampleSize = NES::Util::readValueFromMemRef<uint64_t>(noTuplesMemRef);
    recordPos = 0;

    const auto metaDataSizeRef = statisticMemArea + sizeOfTotalAreaSize;
    const auto metaDataSize = Util::readValueFromMemRef<uint32_t>(metaDataSizeRef);
    sampleData = statisticMemArea + sizeOfTotalAreaSize + sizeOfMetaDataSize + metaDataSize;
}

void ReservoirSampleIteratorImpl::advanceToEnd()
{
    const auto noTuplesMemRef = statisticMemArea + (sizeOfTotalAreaSize + sizeOfMetaDataSize);
    recordPos = NES::Util::readValueFromMemRef<uint64_t>(noTuplesMemRef);
    sampleSize = recordPos;
}

}
