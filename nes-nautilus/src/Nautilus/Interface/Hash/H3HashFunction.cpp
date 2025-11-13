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
#include <Nautilus/Interface/Hash/H3HashFunction.hpp>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>

namespace NES::Nautilus::Interface
{
H3HashFunction::H3HashFunction(const uint64_t sizeSeedInBytes, const uint64_t numberOfBitsInKey, nautilus::val<int8_t*> seeds)
    : sizeSeedInBytes(sizeSeedInBytes), numberOfBitsInKey(numberOfBitsInKey), seeds(std::move(seeds))
{
}

std::unique_ptr<HashFunction> H3HashFunction::clone() const
{
    return std::make_unique<H3HashFunction>(*this);
}

HashFunction::HashValue H3HashFunction::init() const
{
    return 0;
}

HashFunction::HashValue H3HashFunction::calculate(HashValue& hash, const VarVal& value) const
{
    auto operatingValue = value.cast<HashValue>();
    for (nautilus::static_val<uint64_t> keyBit = 0; keyBit < numberOfBitsInKey; ++keyBit)
    {
        auto isBitSet = (operatingValue & 1) == 1;
        operatingValue >>= 1;
        if (isBitSet)
        {
            auto seedRef = seeds + nautilus::val<uint64_t>{keyBit * sizeSeedInBytes};
            auto seedValue = Nautilus::Util::readValueFromMemRef<HashValue::raw_type>(seedRef);
            hash = hash ^ seedValue;
        }
    }

    return hash;
}
}
