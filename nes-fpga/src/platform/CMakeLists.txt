# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#add_subdirectory(avbuf)
#add_subdirectory(axidma)
#add_subdirectory(axipmon)

add_source_files(nes-fpga
        outbyte.c
        xaxidma.c
        xcoresightpsdcc.c
        xil_printf.c
        xuartps_hw.c)