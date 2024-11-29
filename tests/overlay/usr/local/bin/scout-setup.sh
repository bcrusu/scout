#!/usr/bin/env bash

mount_scoutfs() {
    local scoutfs_dev="/dev/vdb"
    local scoutfs_dir="/scout"

    mount -o ro -t ext4 $scoutfs_dev $scoutfs_dir
}

net_up() {
    devs=$(ls /sys/class/net | grep -v lo)
    for dev in $devs; do
        ip link set $dev up
        echo $dev is UP 
    done
}

mount_scoutfs
net_up
