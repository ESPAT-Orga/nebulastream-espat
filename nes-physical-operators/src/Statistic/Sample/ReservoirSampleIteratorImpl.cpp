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
    const nautilus::val<int8_t*>& statisticMemArea, const ReservoirSampleProviderArguments& reservoirSampleArguments)
    : StatisticProviderIteratorImpl(std::move(statisticMemArea)), memoryLayout(reservoirSampleArguments.memoryLayout)
{
}

Nautilus::Record ReservoirSampleIteratorImpl::operator*()
{
    /// As we can not assume that the statistic data is backed by a tuple buffer, we need to manually take care
    /// of variable sized data. For fixed data, we can use the TupleBufferMemoryProvider::read()
    Record record;
    auto fieldAddress = nextTupleMem;
    for (nautilus::static_val<uint64_t> fieldIndex = 0; fieldIndex < memoryLayout->getSchema().getNumberOfFields(); ++fieldIndex)
    {
        const auto fieldName = memoryLayout->getSchema().getFieldAt(fieldIndex).name;
        const auto physicalType = memoryLayout->getPhysicalType(fieldIndex);
        nautilus::val<uint64_t> fieldOffset = physicalType.getSizeInBytes();
        if (physicalType.type == DataType::Type::VARSIZED)
        {
            VariableSizedData varSizedData{fieldAddress};
            record.write(fieldName, varSizedData);
            fieldOffset = varSizedData.getTotalSize();
        }
        else
        {
            const auto varVal = VarVal::readVarValFromMemory(fieldAddress, physicalType.type);
            record.write(fieldName, varVal);
        }
        fieldAddress = fieldAddress + fieldOffset;
    }

    return record;
}

StatisticProviderIteratorImpl& ReservoirSampleIteratorImpl::operator++()
{
    /// As we can not assume that the statistic data is backed by a tuple buffer, we need to manually take care
    /// of variable sized data. For fixed data, we can use the TupleBufferMemoryProvider::read()
    for (nautilus::static_val<uint64_t> fieldIndex = 0; fieldIndex < memoryLayout->getSchema().getNumberOfFields(); ++fieldIndex)
    {
        const auto fieldName = memoryLayout->getSchema().getFieldAt(fieldIndex).name;
        const auto physicalType = memoryLayout->getPhysicalType(fieldIndex);
        nautilus::val<uint64_t> fieldOffset = physicalType.getSizeInBytes();
        if (physicalType.type == DataType::Type::VARSIZED)
        {
            VariableSizedData varSizedData{nextTupleMem};
            fieldOffset = varSizedData.getTotalSize();
        }
        nextTupleMem = nextTupleMem + fieldOffset;
    }
    recordPos += 1;
    return *this;
}

nautilus::val<bool> ReservoirSampleIteratorImpl::operator==(const StatisticProviderIteratorImpl& other) const
{
    if (const auto otherReservoirSample = dynamic_cast<const ReservoirSampleIteratorImpl*>(&other); otherReservoirSample != nullptr)
    {
        /// We need recordPos to compare with the end iterator. As we do not know the size of the sample beforehand,
        /// we can not set nextTupleMem to the last byte and thus, we need recordPos for comparision.
        return recordPos == otherReservoirSample->recordPos and statisticMemArea == otherReservoirSample->statisticMemArea
            and sampleSize == otherReservoirSample->sampleSize and memoryLayout == otherReservoirSample->memoryLayout;
    }
    return false;
}

void ReservoirSampleIteratorImpl::advanceToBegin()
{
    ReservoirSampleHeaderRef header;
    header.setMemArea(statisticMemArea);
    sampleSize = header.readSampleSize();
    nextTupleMem = header.getSampleMemArea();
    recordPos = 0;
}

void ReservoirSampleIteratorImpl::advanceToEnd()
{
    ReservoirSampleHeaderRef header;
    header.setMemArea(statisticMemArea);
    sampleSize = header.readSampleSize();
    recordPos = sampleSize;
}

nautilus::val<uint64_t> ReservoirSampleHeaderRef::getTotalSize(const nautilus::val<uint64_t>& sampleDataSize)
{
    totalSize = sampleDataSize + metaDataSize + sizeOfMetaDataSize + sizeOfTotalSize;
    return totalSize;
}

void ReservoirSampleHeaderRef::setMemArea(const nautilus::val<int8_t*>& statisticMemArea)
{
    this->statisticMemArea = statisticMemArea;
}

void ReservoirSampleHeaderRef::setSampleSize(const nautilus::val<uint64_t>& sampleSize)
{
    this->sampleSize = sampleSize;
}

void ReservoirSampleHeaderRef::writeMetaData()
{
    VarVal{totalSize}.writeToMemory(statisticMemArea);
    VarVal{metaDataSize}.writeToMemory(statisticMemArea + sizeOfTotalSize);
    VarVal{sampleSize}.writeToMemory(statisticMemArea + sizeOfTotalSize + sizeOfMetaDataSize);
}

nautilus::val<uint32_t> ReservoirSampleHeaderRef::readMetaDataSize()
{
    return Util::readValueFromMemRef<uint32_t>(statisticMemArea + sizeOfTotalSize);
}

nautilus::val<uint64_t> ReservoirSampleHeaderRef::readSampleSize()
{
    const auto sampleMemRef = statisticMemArea + sizeOfTotalSize + sizeOfMetaDataSize;
    return Util::readValueFromMemRef<uint64_t>(sampleMemRef);
}

nautilus::val<int8_t*> ReservoirSampleHeaderRef::getSampleMemArea()
{
    return statisticMemArea + sizeOfTotalSize + sizeOfMetaDataSize + metaDataSize;
}

nautilus::val<uint64_t> ReservoirSampleHeaderRef::readTotalSize()
{
    return Util::readValueFromMemRef<uint64_t>(statisticMemArea);
}

}
