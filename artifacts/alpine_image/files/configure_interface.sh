#!/bin/ash

set -x
exec > /var/log/configure_interface.log 2>&1

CIDR=$(jq -r .meta.cidr /var/lib/cloud/openstack/latest/meta_data.json)

while true; do
    for pci_bus in $(ls /sys/class/pci_bus); do
        echo 1 > "/sys/class/pci_bus/${pci_bus}/rescan"
    done

    LINK=$(ip link | grep -o eth1)
    if [ -z ${LINK} ]; then
        # There is no device yet => nothing to do
        sleep 5
        continue
    fi

    IP=$(ip -f inet addr show dev eth1 | egrep -o \
            '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\/[0-9]+')
    if [ ! -z ${IP} ]; then
        # Device already have IP address
        echo "eth1 already configured"
        break
    else
        ip link set dev eth1 up
        ip addr add ${CIDR} dev eth1
        break
    fi
done
