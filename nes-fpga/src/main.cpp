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

#include <cstdint>
#include <cstring>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

/// AXI DMA REGISTER MAP

/// Offsets (from PG021)
constexpr uint32_t MM2S_DMACR = 0x00;
constexpr uint32_t MM2S_DMASR = 0x04;
constexpr uint32_t MM2S_SA = 0x18;
constexpr uint32_t MM2S_LENGTH = 0x28;

constexpr uint32_t S2MM_DMACR = 0x30;
constexpr uint32_t S2MM_DMASR = 0x34;
constexpr uint32_t S2MM_DA = 0x48;
constexpr uint32_t S2MM_LENGTH = 0x58;

/// Control bits
constexpr uint32_t DMACR_RUNSTOP = 1 << 0;
constexpr uint32_t DMASR_IDLE = 1 << 1;

/// Test parameters
constexpr size_t BUF_SIZE = 32;
constexpr uint8_t TEST_START_VALUE = 0x0C;

int main()
{
    /// Open UIO
    int uio_fd = open("/dev/uio0", O_RDWR);
    if (uio_fd < 0)
    {
        perror("open /dev/uio0");
        return 1;
    }

    /// Map DMA registers
    auto* dma = static_cast<volatile uint32_t*>(mmap(nullptr, 0x10000, PROT_READ | PROT_WRITE, MAP_SHARED, uio_fd, 0));

    if (dma == MAP_FAILED)
    {
        perror("mmap dma regs");
        return 1;
    }

    /// Open udmabuf
    int buf_fd = open("/dev/udmabuf0", O_RDWR);
    if (buf_fd < 0)
    {
        perror("open udmabuf");
        return 1;
    }

    auto* buf = static_cast<uint8_t*>(mmap(nullptr, BUF_SIZE * 2, PROT_READ | PROT_WRITE, MAP_SHARED, buf_fd, 0));

    /// TX = first half, RX = second half
    uint8_t* tx = buf;
    uint8_t* rx = buf + BUF_SIZE;

    /// Get physical address
    FILE* f = fopen("/sys/class/udmabuf/udmabuf0/phys_addr", "r");
    uint64_t phys;
    fscanf(f, "%lx", &phys);
    fclose(f);

    uint64_t tx_phys = phys;
    uint64_t rx_phys = phys + BUF_SIZE;

    /// Fill TX buffer
    uint8_t val = TEST_START_VALUE;
    for (size_t i = 0; i < BUF_SIZE; i++)
        tx[i] = val++;

    std::memset(rx, 0, BUF_SIZE);

    /// Reset & start DMA
    dma[MM2S_DMACR / 4] = DMACR_RUNSTOP;
    dma[S2MM_DMACR / 4] = DMACR_RUNSTOP;

    /// Program S2MM (RX first)
    dma[S2MM_DA / 4] = static_cast<uint32_t>(rx_phys);
    dma[S2MM_LENGTH / 4] = BUF_SIZE;

    /// Program MM2S
    dma[MM2S_SA / 4] = static_cast<uint32_t>(tx_phys);
    dma[MM2S_LENGTH / 4] = BUF_SIZE;

    /// Poll until idle
    while (!(dma[MM2S_DMASR / 4] & DMASR_IDLE))
    {
    }
    while (!(dma[S2MM_DMASR / 4] & DMASR_IDLE))
    {
    }

    /// Verify
    val = TEST_START_VALUE;
    for (size_t i = 0; i < BUF_SIZE; i++)
    {
        if (rx[i] != val++)
        {
            std::cerr << "Mismatch at " << i << std::endl;
            return 1;
        }
    }

    std::cout << "DMA loopback successful!" << std::endl;
    return 0;
}
