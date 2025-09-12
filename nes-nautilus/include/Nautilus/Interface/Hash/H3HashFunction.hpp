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
#include <Nautilus/Interface/Hash/HashFunction.hpp>

namespace NES::Nautilus::Interface
{
/// H3 implementation taken from the paper Universal classes of hash functions by Carter, J., and Wegman, M. N. Journal of Computer and System Sciences 18, 2 (apr 1979)
class H3HashFunction final : public HashFunction
{
public:
    H3HashFunction(uint64_t sizeSeedInBytes, uint64_t numberOfBitsInKey, nautilus::val<int8_t*> seeds);
    ~H3HashFunction() override = default;
    [[nodiscard]] std::unique_ptr<HashFunction> clone() const override;

protected:
    [[nodiscard]] HashValue init() const override;
    HashValue calculate(HashValue& hash, const VarVal& value) const override;

private:
    uint64_t sizeSeedInBytes;
    uint64_t numberOfBitsInKey;
    nautilus::val<int8_t*> seeds;
};
}
