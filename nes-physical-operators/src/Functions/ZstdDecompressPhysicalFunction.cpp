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

#include <Functions/ZstdDecompressPhysicalFunction.hpp>

#include <exception>
#include <utility>
#include <vector>
#include <zstd.h>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

VarVal ZstdDecompressPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childPhysicalFunction.execute(record, arena);
    nautilus::val<uint32_t> decompressedSize{0};
    nautilus::val<uint8_t*> memDecompressed{nullptr};
    const auto varSizedValueCompressed = value.cast<VariableSizedData>();

    if (type.type == DataType::Type::VARSIZED)
    {
        /// retrieve the size of the original data from zstd's metadata
        decompressedSize = nautilus::invoke(
            +[](const uint8_t* compressed, size_t inputSize) { return static_cast<uint64_t>(ZSTD_getFrameContentSize(compressed, inputSize)); },
            varSizedValueCompressed.getContent(),
            varSizedValueCompressed.getContentSize());

        /// if it is possible to perform an arena allocation outside of nautilus, the number of invokes could be reduced to one for the
        /// whole function
        const auto decompressedVarSizedTotalSize = decompressedSize + nautilus::val<size_t>(sizeof(uint32_t));
        memDecompressed = arena.allocateMemory(decompressedVarSizedTotalSize);

        nautilus::invoke(
            decompressVarSized,
            varSizedValueCompressed.getContentSize(),
            varSizedValueCompressed.getContent(),
            decompressedSize,
            memDecompressed);

        return VariableSizedData(memDecompressed);
    }
    else
    {
        decompressedSize = nautilus::val<uint32_t>(type.getSizeInBytes());

        memDecompressed = arena.allocateMemory(decompressedSize);
        nautilus::invoke(
            decompressFixedSize,
            varSizedValueCompressed.getContentSize(),
            varSizedValueCompressed.getContent(),
            decompressedSize,
            memDecompressed);

        return VarVal::readVarValFromMemory(memDecompressed, type.type);
    }
}

ZstdDecompressPhysicalFunction::ZstdDecompressPhysicalFunction(PhysicalFunction childPhysicalFunction, DataType type)
    : childPhysicalFunction(childPhysicalFunction), type(type)
{
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterZstdDecompressPhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1, "Zstd compression function must have exactly one sub-function");
    const auto function = physicalFunctionRegistryArguments.childFunctions[0];
    return ZstdDecompressPhysicalFunction(function, physicalFunctionRegistryArguments.outputType);
}

void ZstdDecompressPhysicalFunction::decompressVarSized(
    size_t inputSize, const int8_t* inputData, size_t decompressedSize, int8_t* decompressedData)
{
    *std::bit_cast<uint32_t*>(decompressedData) = static_cast<uint32_t>(decompressedSize);
    const size_t actualSize = ZSTD_decompress(decompressedData + sizeof(uint32_t), decompressedSize, inputData, inputSize);
    if (ZSTD_isError(actualSize))
    {
        NES_DEBUG("Zstd Decompression error: {}", ZSTD_getErrorName(actualSize));
        throw std::exception();
    }
}

void ZstdDecompressPhysicalFunction::decompressFixedSize(
    size_t inputSize, const int8_t* inputData, size_t decompressedSize, int8_t* decompressedData)
{
    const size_t actualSize = ZSTD_decompress(decompressedData, decompressedSize, inputData, inputSize);
    if (ZSTD_isError(actualSize))
    {
        NES_DEBUG("Zstd Decompression error: {}", ZSTD_getErrorName(actualSize));
        throw std::exception();
    }
}
}
