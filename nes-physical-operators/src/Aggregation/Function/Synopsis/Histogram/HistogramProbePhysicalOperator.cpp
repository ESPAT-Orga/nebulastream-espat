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

#include <Aggregation/Function/Synopsis/Histogram/HistogramProbePhysicalOperator.hpp>

#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/Synopsis/SynopsisFunctionRef.hpp>
#include <function.hpp>

namespace NES
{

HistogramProbePhysicalOperator::HistogramProbePhysicalOperator(
    const Schema& bucketSchema, const Record::RecordFieldIdentifier& fieldIdentifier, WindowMetaData windowMetaData)
    : bucketSchema(bucketSchema), inputFieldIdentifier(fieldIdentifier), windowMetaData(windowMetaData)
{
}

void HistogramProbePhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto synRef = SynopsisFunctionRef(bucketSchema);
    synRef.initializeForReading(record.read(inputFieldIdentifier).cast<VariableSizedData>().getReference());
    auto histogramRecord = synRef.readNextRecord();
    histogramRecord.write(windowMetaData.windowStartFieldName, record.read(windowMetaData.windowStartFieldName));
    histogramRecord.write(windowMetaData.windowEndFieldName, record.read(windowMetaData.windowEndFieldName));
    executeChild(executionCtx, histogramRecord);
}

std::optional<PhysicalOperator> HistogramProbePhysicalOperator::getChild() const
{
    return child;
}

void HistogramProbePhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}


}
