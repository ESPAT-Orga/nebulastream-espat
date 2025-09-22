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

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

class ZstdCompressPhysicalFunction final : public PhysicalFunctionConcept
{
public:
    ZstdCompressPhysicalFunction(PhysicalFunction childPhysicalFunction, DataType type, uint32_t compressionLevel);
    size_t static compress(
        size_t inputSize, int8_t* inputData, size_t compressedMaxSize, int8_t* compressedData, uint32_t compressionLevel);
    static void copyCompressionResultAndSize(int8_t* destination, int8_t* source, size_t compressedSize);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    PhysicalFunction childPhysicalFunction;
    DataType type;
    uint32_t compressionLevel;
};
}
