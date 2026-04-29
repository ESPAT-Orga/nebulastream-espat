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

#pragma once

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>

namespace NES
{

/// Re-derives CountMinSketch's H3 column index for a given (value, rowIndex)
/// using the same kCountMinSeed-based mt19937 byte stream the build pipeline
/// uses (see CountMinSketchPhysicalFunction::reset). Lets accuracy queries
/// pick the right (rowIndex, columnIndex) cells out of COUNTMIN_PROBE.
class H3HashPhysicalFunction final
{
public:
    H3HashPhysicalFunction(
        PhysicalFunction valueFunction,
        PhysicalFunction rowIndexFunction,
        PhysicalFunction numberOfRowsFunction,
        PhysicalFunction numberOfColsFunction,
        PhysicalFunction seedFunction);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction valueFunction;
    PhysicalFunction rowIndexFunction;
    PhysicalFunction numberOfRowsFunction;
    PhysicalFunction numberOfColsFunction;
    PhysicalFunction seedFunction;
};

static_assert(PhysicalFunctionConcept<H3HashPhysicalFunction>);

}
