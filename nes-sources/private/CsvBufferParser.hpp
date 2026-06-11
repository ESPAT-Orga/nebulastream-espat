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

#include <cstddef>
#include <string>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// Native row layout of a tuple, derived purely from the schema and the configured buffer size.
/// Shared by MemorySource (single file) and LoopingMemorySource (multi file), so the byte offsets
/// and tuple size are computed once and reused (LoopingMemorySource needs the offsets to resolve
/// its monotonic-timestamp field and the tuple size for its per-replay patch).
struct CsvRowLayout
{
    /// Byte offset of each schema field within a row.
    std::vector<size_t> fieldByteOffsets;
    /// Bytes per native tuple.
    size_t tupleSize;
    /// How many tuples fit in one buffer of bufferSizeInBytes.
    size_t tuplesPerBuffer;
};

/// Compute the native row layout. Throws InvalidConfigParameter for an empty schema and
/// TuplesTooLargeForPipelineBufferSize when a single tuple exceeds the buffer size.
[[nodiscard]] CsvRowLayout computeCsvRowLayout(const Schema& schema, size_t bufferSizeInBytes);

/// Parse a CSV file into native row-layout TupleBuffers, appending them to outBuffers. Buffers are
/// unpooled allocations of bufferSizeInBytes from bufferProvider, so parsing the whole file during
/// setup() never competes with the source's bounded local pool. Records are split on the parser's
/// single-character tuple/field delimiters; VARSIZED fields are stored in child buffers.
void parseCsvFileIntoBuffers(
    const std::string& filePath,
    const Schema& schema,
    const CsvRowLayout& layout,
    const ParserConfig& parserConfig,
    size_t bufferSizeInBytes,
    AbstractBufferProvider& bufferProvider,
    std::vector<TupleBuffer>& outBuffers);

}
