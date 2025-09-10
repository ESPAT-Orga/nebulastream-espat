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

#include <Aggregation/Function/Synopsis/Histogram/HistogramPhysicalFunction.hpp>

namespace NES
{

HistogramPhysicalFunction::HistogramPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

std::string HistogramPhysicalFunction::createBucketSchemaKeyName(size_t index)
{
    return fmt::format("bucket_{}_lower", index);
}

std::string HistogramPhysicalFunction::createBucketSchemaValueName(size_t index)
{
    return fmt::format("bucket_{}_count", index);
}

Schema HistogramPhysicalFunction::createBucketSchema(uint64_t numBuckets)
{
    Schema bucketSchema;
    for (uint64_t i = 0; i < numBuckets; i++)
    {
        bucketSchema.addField(createBucketSchemaKeyName(i), DataType::Type::UINT64);
        bucketSchema.addField(createBucketSchemaValueName(i), DataType::Type::UINT64);
    }
    return bucketSchema;
}

}
