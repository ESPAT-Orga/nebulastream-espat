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

#include <Statistic/Scalar/ScalarStatisticIteratorImpl.hpp>

namespace NES
{

ScalarStatisticIteratorImpl::ScalarStatisticIteratorImpl(
    const nautilus::val<int8_t*>& statisticMemArea, ScalarStatisticProviderArguments scalarProviderArguments)
    : StatisticProviderIteratorImpl(std::move(statisticMemArea)), scalarProviderArgs(std::move(scalarProviderArguments))
{
}

Record ScalarStatisticIteratorImpl::operator*()
{
    /// The value is the whole payload, so it sits at offset 0 with no header to skip
    Record record;
    record.write(
        scalarProviderArgs.valueFieldName, VarVal::readNonNullableVarValFromMemory(statisticMemArea, scalarProviderArgs.valueDataType));
    return record;
}

StatisticProviderIteratorImpl& ScalarStatisticIteratorImpl::operator++()
{
    index += 1;
    return *this;
}

nautilus::val<bool> ScalarStatisticIteratorImpl::operator==(const StatisticProviderIteratorImpl& other) const
{
    if (const auto otherScalar = dynamic_cast<const ScalarStatisticIteratorImpl*>(&other); otherScalar != nullptr)
    {
        return index == otherScalar->index and statisticMemArea == otherScalar->statisticMemArea;
    }
    return false;
}

void ScalarStatisticIteratorImpl::advanceToBegin()
{
    index = 0;
}

void ScalarStatisticIteratorImpl::advanceToEnd()
{
    index = 1;
}

}
