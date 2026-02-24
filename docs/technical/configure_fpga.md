# Configuring the FPGA

This document explains the process of configuring the Ubuntu OS running on the Zynq Ultrascale ZCU106.

## Program the FPGA

Run the following command in the terminal to program the FPGA:

```shell
sudo fpgautil -b <design.bit>
```

## Configure UIO

Create a `dma_overlay.dts` file and copy the followign content to it:

```
/dts-v1/;
/plugin/;

/ {
    fragment@0 {
        target-path = "/axi";
        __overlay__ {

            axi_dma_0: dma@a0000000 {
                compatible = "generic-uio";
                reg = <0x0 0xa0000000 0x0 0x00010000>;
                status = "okay";
            };
        };
    };
};
```

Now, compile and load it: 

```shell
sudo dtc -@ -I dts -O dtb -o dma_overlay.dtbo dma_overlay.dts
sudo rmdir /sys/kernel/config/device-tree/overlays/dma
sudo mkdir /sys/kernel/config/device-tree/overlays/dma
sudo sh -c "cat dma_overlay.dtbo > /sys/kernel/config/device-tree/overlays/dma/dtbo"
```

And check the system:

```shell
ls /dev/uio*
```

and

```shell
for i in /sys/class/uio/uio*; do
    echo "---- $i ----"
    cat $i/name
    cat $i/maps/map0/addr
done
```

It should show the following address for one of the devices:

```
0x00000000a0000000
```
