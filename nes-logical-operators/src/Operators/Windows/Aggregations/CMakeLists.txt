# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-logical-operators
        AggregationLogicalFunctionRegistry.cpp
        WindowAggregationLogicalFunction.cpp
        AvgAggregationLogicalFunction.cpp
        CountAggregationLogicalFunction.cpp
        MaxAggregationLogicalFunction.cpp
        MedianAggregationLogicalFunction.cpp
        MinAggregationLogicalFunction.cpp
        SumAggregationLogicalFunction.cpp
)


add_unreflection_entry(AggregationLogicalFunction Avg)
add_unreflection_entry(AggregationLogicalFunction Count)
add_unreflection_entry(AggregationLogicalFunction Max)
add_unreflection_entry(AggregationLogicalFunction Median)
add_unreflection_entry(AggregationLogicalFunction Min)
add_unreflection_entry(AggregationLogicalFunction Sum)

add_registry_entry(AggregationLogicalFunction Avg)
add_registry_entry(AggregationLogicalFunction Count)
add_registry_entry(AggregationLogicalFunction Max)
add_registry_entry(AggregationLogicalFunction Median)
add_registry_entry(AggregationLogicalFunction Min)
add_registry_entry(AggregationLogicalFunction Sum)

add_source_files(nes-logical-operators
        Sample/ReservoirSampleLogicalFunction.cpp
        Histogram/EquiWidthHistogramLogicalFunction.cpp
        Histogram/EquiWidthHistogramDeltaGenLogicalFunction.cpp
        Histogram/EquiWidthHistogramDeltaResolverLogicalFunction.cpp
        Sketch/CountMinSketchLogicalFunction.cpp
        Scalar/ScalarStatisticLogicalFunction.cpp
        Sample/ReservoirProbeLogicalOperator.cpp
        Histogram/EquiWidthHistogramProbeLogicalOperator.cpp
        Sketch/CountMinSketchProbeLogicalOperator.cpp
        Scalar/ScalarStatisticProbeLogicalOperator.cpp
        StatisticLogicalFunction.cpp)

add_unreflection_entry(AggregationLogicalFunction ReservoirSample HEADER Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp)
add_unreflection_entry(AggregationLogicalFunction EquiWidthHistogram HEADER Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp)
add_unreflection_entry(AggregationLogicalFunction EquiWidthHistogramDeltaGen HEADER Operators/Windows/Aggregations/Histogram/EquiWidthHistogramDeltaGenLogicalFunction.hpp)
add_unreflection_entry(AggregationLogicalFunction EquiWidthHistogramDeltaResolver HEADER Operators/Windows/Aggregations/Histogram/EquiWidthHistogramDeltaResolverLogicalFunction.hpp)
add_unreflection_entry(AggregationLogicalFunction CountMinSketch HEADER Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp)
add_unreflection_entry(AggregationLogicalFunction ScalarStatistic HEADER Operators/Windows/Aggregations/Scalar/ScalarStatisticLogicalFunction.hpp)
add_unreflection_entry(LogicalOperator ReservoirProbe HEADER Operators/Windows/Aggregations/Sample/ReservoirProbeLogicalOperator.hpp)
add_unreflection_entry(LogicalOperator EquiWidthHistogramProbe HEADER Operators/Windows/Aggregations/Histogram/EquiWidthHistogramProbeLogicalOperator.hpp)
add_unreflection_entry(LogicalOperator CountMinSketchProbe HEADER Operators/Windows/Aggregations/Sketch/CountMinSketchProbeLogicalOperator.hpp)
add_unreflection_entry(LogicalOperator ScalarStatisticProbe HEADER Operators/Windows/Aggregations/Scalar/ScalarStatisticProbeLogicalOperator.hpp)

add_registry_entry(AggregationLogicalFunction ReservoirSample HEADER Operators/Windows/Aggregations/Sample/ReservoirSampleLogicalFunction.hpp)
add_registry_entry(AggregationLogicalFunction EquiWidthHistogram HEADER Operators/Windows/Aggregations/Histogram/EquiWidthHistogramLogicalFunction.hpp)
add_registry_entry(AggregationLogicalFunction EquiWidthHistogramDeltaGen HEADER Operators/Windows/Aggregations/Histogram/EquiWidthHistogramDeltaGenLogicalFunction.hpp)
add_registry_entry(AggregationLogicalFunction EquiWidthHistogramDeltaResolver HEADER Operators/Windows/Aggregations/Histogram/EquiWidthHistogramDeltaResolverLogicalFunction.hpp)
add_registry_entry(AggregationLogicalFunction CountMinSketch HEADER Operators/Windows/Aggregations/Sketch/CountMinSketchLogicalFunction.hpp)
add_registry_entry(AggregationLogicalFunction ScalarStatistic HEADER Operators/Windows/Aggregations/Scalar/ScalarStatisticLogicalFunction.hpp)
add_registry_entry(LogicalOperator ReservoirProbe HEADER Operators/Windows/Aggregations/Sample/ReservoirProbeLogicalOperator.hpp)
add_registry_entry(LogicalOperator EquiWidthHistogramProbe HEADER Operators/Windows/Aggregations/Histogram/EquiWidthHistogramProbeLogicalOperator.hpp)
add_registry_entry(LogicalOperator CountMinSketchProbe HEADER Operators/Windows/Aggregations/Sketch/CountMinSketchProbeLogicalOperator.hpp)
add_registry_entry(LogicalOperator ScalarStatisticProbe HEADER Operators/Windows/Aggregations/Scalar/ScalarStatisticProbeLogicalOperator.hpp)
