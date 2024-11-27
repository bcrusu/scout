#!/usr/bin/env bash

main() {
    devs=$(ls /sys/class/net | grep -v lo)
    for dev in $devs; do
        ip link set $dev up
        echo $dev is UP 
    done
}

main