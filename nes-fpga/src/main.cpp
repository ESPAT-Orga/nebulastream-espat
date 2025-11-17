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

/***************************** Include Files *********************************/
#include <include/platform/sleep.h>
#include <include/platform/xaxidma.h>
#include <include/platform/xdebug.h>
#include <include/platform/xparameters.h>

#if defined(XPAR_UARTNS550_0_BASEADDR)
    #include "xuartns550_l.h" /* to use uartns550 */
#endif

/******************** Constant Definitions **********************************/

#define SDT

/*
 * Device hardware build related constants.
 */

#ifndef SDT
    #define DMA_DEV_ID XPAR_AXIDMA_0_DEVICE_ID

    #ifdef XPAR_AXI_7SDDR_0_S_AXI_BASEADDR
        #define DDR_BASE_ADDR XPAR_AXI_7SDDR_0_S_AXI_BASEADDR
    #elif defined(XPAR_MIG7SERIES_0_BASEADDR)
        #define DDR_BASE_ADDR XPAR_MIG7SERIES_0_BASEADDR
    #elif defined(XPAR_MIG_0_C0_DDR4_MEMORY_MAP_BASEADDR)
        #define DDR_BASE_ADDR XPAR_MIG_0_C0_DDR4_MEMORY_MAP_BASEADDR
    #elif defined(XPAR_PSU_DDR_0_S_AXI_BASEADDR)
        #define DDR_BASE_ADDR XPAR_PSU_DDR_0_S_AXI_BASEADDR
    #endif

#else

    #ifdef XPAR_MEM0_BASEADDRESS
        #define DDR_BASE_ADDR XPAR_MEM0_BASEADDRESS
    #endif
#endif

#ifndef DDR_BASE_ADDR
    #warning CHECK FOR THE VALID DDR ADDRESS IN XPARAMETERS.H, \
DEFAULT SET TO 0x01000000
    #define MEM_BASE_ADDR 0x01000000
#else
    #define MEM_BASE_ADDR (DDR_BASE_ADDR + 0x1000000)
#endif

#define TX_BUFFER_BASE (MEM_BASE_ADDR + 0x00100000)
#define RX_BUFFER_BASE (MEM_BASE_ADDR + 0x00300000)
#define RX_BUFFER_HIGH (MEM_BASE_ADDR + 0x004FFFFF)

#define MAX_PKT_LEN 0x20

#define TEST_START_VALUE 0xC

#define NUMBER_OF_TRANSFERS 10
#define POLL_TIMEOUT_COUNTER 1000000U

/**************************** Type Definitions *******************************/


/***************** Macros (Inline Functions) Definitions *********************/


/************************** Function Prototypes ******************************/

#if (!defined(DEBUG))
extern void xil_printf(const char* format, ...);
#endif

#ifndef SDT
int XAxiDma_SimplePollExample(u16 DeviceId);
#else
int XAxiDma_SimplePollExample(UINTPTR BaseAddress);
#endif
static int CheckData(void);

/************************** Variable Definitions *****************************/
/*
 * Device instance definitions
 */
XAxiDma AxiDma;

/*****************************************************************************/
/**
* The entry point for this example. It invokes the example function,
* and reports the execution status.
*
* @param	None.
*
* @return
*		- XST_SUCCESS if example finishes successfully
*		- XST_FAILURE if example fails.
*
* @note		None.
*
******************************************************************************/
int main()
{
    int Status;

    xil_printf("\r\n--- Entering main() --- \r\n");

    /* Run the poll example for simple transfer */
#ifndef SDT
    Status = XAxiDma_SimplePollExample(DMA_DEV_ID);
#else
    Status = XAxiDma_SimplePollExample(XPAR_XAXIDMA_0_BASEADDR);
#endif

    if (Status != XST_SUCCESS)
    {
        xil_printf("XAxiDma_SimplePoll Example Failed\r\n");
        return XST_FAILURE;
    }

    xil_printf("Successfully ran XAxiDma_SimplePoll Example\r\n");

    xil_printf("--- Exiting main() --- \r\n");

    return XST_SUCCESS;
}

#if defined(XPAR_UARTNS550_0_BASEADDR)
/*****************************************************************************/
/*
*
* Uart16550 setup routine, need to set baudrate to 9600, and data bits to 8
*
* @param	None.
*
* @return	None
*
* @note		None.
*
******************************************************************************/
static void Uart550_Setup(void)
{
    /* Set the baudrate to be predictable
	 */
    XUartNs550_SetBaud(XPAR_UARTNS550_0_BASEADDR, XPAR_XUARTNS550_CLOCK_HZ, 9600);

    XUartNs550_SetLineControlReg(XPAR_UARTNS550_0_BASEADDR, XUN_LCR_8_DATA_BITS);
}
#endif

/*****************************************************************************/
/**
* The example to do the simple transfer through polling. The constant
* NUMBER_OF_TRANSFERS defines how many times a simple transfer is repeated.
*
* @param	DeviceId is the Device Id of the XAxiDma instance
*
* @return
*		- XST_SUCCESS if example finishes successfully
*		- XST_FAILURE if error occurs
*
* @note		None
*
*
******************************************************************************/
#ifndef SDT
int XAxiDma_SimplePollExample(u16 DeviceId)
#else
int XAxiDma_SimplePollExample(UINTPTR BaseAddress)
#endif
{
    XAxiDma_Config* CfgPtr;
    int Status;
    int Tries = NUMBER_OF_TRANSFERS;
    int Index;
    u8* TxBufferPtr;
    u8* RxBufferPtr;
    u8 Value;
    int TimeOut = POLL_TIMEOUT_COUNTER;

    TxBufferPtr = (u8*)TX_BUFFER_BASE;
    RxBufferPtr = (u8*)RX_BUFFER_BASE;

    /* Initialize the XAxiDma device.
	 */
#ifndef SDT
    CfgPtr = XAxiDma_LookupConfig(DeviceId);
    if (!CfgPtr)
    {
        xil_printf("No config found for %d\r\n", DeviceId);
        return XST_FAILURE;
    }
#else
    CfgPtr = XAxiDma_LookupConfig(BaseAddress);
    if (!CfgPtr)
    {
        xil_printf("No config found for %d\r\n", BaseAddress);
        return XST_FAILURE;
    }
#endif

    Status = XAxiDma_CfgInitialize(&AxiDma, CfgPtr);
    if (Status != XST_SUCCESS)
    {
        xil_printf("Initialization failed %d\r\n", Status);
        return XST_FAILURE;
    }

    if (XAxiDma_HasSg(&AxiDma))
    {
        xil_printf("Device configured as SG mode \r\n");
        return XST_FAILURE;
    }

    /* Disable interrupts, we use polling mode
	 */
    XAxiDma_IntrDisable(&AxiDma, XAXIDMA_IRQ_ALL_MASK, XAXIDMA_DEVICE_TO_DMA);
    XAxiDma_IntrDisable(&AxiDma, XAXIDMA_IRQ_ALL_MASK, XAXIDMA_DMA_TO_DEVICE);

    Value = TEST_START_VALUE;

    for (Index = 0; Index < MAX_PKT_LEN; Index++)
    {
        TxBufferPtr[Index] = Value;

        Value = (Value + 1) & 0xFF;
    }
    /* Flush the buffers before the DMA transfer, in case the Data Cache
	 * is enabled
	 */
    Xil_DCacheFlushRange((UINTPTR)TxBufferPtr, MAX_PKT_LEN);
    Xil_DCacheFlushRange((UINTPTR)RxBufferPtr, MAX_PKT_LEN);

    for (Index = 0; Index < Tries; Index++)
    {
        Status = XAxiDma_SimpleTransfer(&AxiDma, (UINTPTR)RxBufferPtr, MAX_PKT_LEN, XAXIDMA_DEVICE_TO_DMA);

        if (Status != XST_SUCCESS)
        {
            return XST_FAILURE;
        }

        Status = XAxiDma_SimpleTransfer(&AxiDma, (UINTPTR)TxBufferPtr, MAX_PKT_LEN, XAXIDMA_DMA_TO_DEVICE);

        if (Status != XST_SUCCESS)
        {
            return XST_FAILURE;
        }

        /*Wait till tranfer is done or 1usec * 10^6 iterations of timeout occurs*/
        while (TimeOut)
        {
            if (!(XAxiDma_Busy(&AxiDma, XAXIDMA_DEVICE_TO_DMA)) && !(XAxiDma_Busy(&AxiDma, XAXIDMA_DMA_TO_DEVICE)))
            {
                break;
            }
            TimeOut--;
            usleep(1U);
        }

        Status = CheckData();
        if (Status != XST_SUCCESS)
        {
            return XST_FAILURE;
        }
    }

    /* Test finishes successfully
	 */
    return XST_SUCCESS;
}

/*****************************************************************************/
/*
*
* This function checks data buffer after the DMA transfer is finished.
*
* @param	None
*
* @return
*		- XST_SUCCESS if validation is successful.
*		- XST_FAILURE otherwise.
*
* @note		None.
*
******************************************************************************/
static int CheckData(void)
{
    u8* RxPacket;
    int Index = 0;
    u8 Value;

    RxPacket = (u8*)RX_BUFFER_BASE;
    Value = TEST_START_VALUE;

    /* Invalidate the DestBuffer before receiving the data, in case the
	 * Data Cache is enabled
	 */
    Xil_DCacheInvalidateRange((UINTPTR)RxPacket, MAX_PKT_LEN);

    for (Index = 0; Index < MAX_PKT_LEN; Index++)
    {
        if (RxPacket[Index] != Value)
        {
            xil_printf("Data error %d: %x/%x\r\n", Index, (unsigned int)RxPacket[Index], (unsigned int)Value);

            return XST_FAILURE;
        }
        Value = (Value + 1) & 0xFF;
    }

    return XST_SUCCESS;
}
