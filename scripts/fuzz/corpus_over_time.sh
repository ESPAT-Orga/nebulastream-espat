#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

DURATION=10m

if [ ! -d /out ]
then
    echo expects /out dir to exist
    exit 1
fi

if [ $# != 2 ]
then
    echo usage: $0 engine harness
    exit 1
fi

engine=$1
harness=$2

if [ $harness != "sql-parser-simple-fuzz" ] && [ $harness != "snw-text-fuzz" ] && [ $harness != "snw-proto-fuzz" ] && [ $harness != "snw-strict-fuzz" ]
then
    echo unknown harness $harness, expexting one of
    echo
    echo sql-parser-simple-fuzz
    echo snw-text-fuzz
    echo snw-proto-fuzz
    echo snw-strict-fuzz
    exit 1
fi

if [ $harness == "sql-parser-simple-fuzz" ] && [ $harness == "snw-text-fuzz" ]
then
    input_type=nesql
else
    input_type=txtpb
fi


git clone --depth=1 --branch=fuzz https://github.com/nebulastream/nebulastream.git
cd nebulastream || exit 1

cmake -B systest-build -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
cmake --build systest-build --target systest

mkdir queries
cd queries
../systest-build/nes-systests/systest/systest --dump-queries

mkdir /corpus-nesql
mkdir /corpus-txtpb

mv ./*.nesql /corpus-nesql
mv ./*.txtpb /corpus-txtpb

cd ..


if [ $engine = "libfuzzer" ]
then
    true
elif [ $engine = "aflpp" ]
then
    export AFL_SKIP_CPUFREQ=1
    export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1

    export CC=afl-clang-lto
    export CXX=afl-clang-lto++
elif [ $engine = "honggfuzz" ]
then
    export CC=hfuzz-clang
    export CXX=hfuzz-clang++
else
    echo unknown engine $engine. Must be one of libfuzzer, aflpp, or honggfuzz
    exit 1
fi

cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
cmake --build build --target $harness
harness=$(pwd)/$(find -name $harness)

cd /out

git init .
git config user.name "peter"
git config user.email "peter@enternet.de"

while true
do
    git add .
    git commit -m foo
    sleep 60
done &

if [ $engine = "libfuzzer" ]
then
    mkdir corpus
    timeout $DURATION $harness -jobs=100000 -workers=1 /out/corpus /corpus-$input_type
elif [ $engine = "aflpp" ]
then
    timeout $DURATION afl-fuzz -t 10000 -i /corpus-$input_type -o /out/corpus -- $harness
elif [ $engine = "honggfuzz" ]
    timeout $DURATION honggfuzz         -i /corpus-$input_type -o /out/corpus -- $harness
then
    echo todo
else
    echo wat
    exit 1
fi

kill %1
