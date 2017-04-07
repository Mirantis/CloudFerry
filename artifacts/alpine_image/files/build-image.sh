#!/bin/ash

REPO="http://dl-cdn.alpinelinux.org"

# Build ZSTD
cd /root
tar zxf /root/zstd-1.0.0.tar.gz
cd /root/zstd-1.0.0
make
cd /

qemu-img create -f raw /tmp/alpine_vol_tx.raw 256M
parted -s /tmp/alpine_vol_tx.raw mklabel msdos
parted -s /tmp/alpine_vol_tx.raw mkpart primary ext2 0% 100%
parted -s /tmp/alpine_vol_tx.raw set 1 boot on

OFFSET=$(parted -s /tmp/alpine_vol_tx.raw unit B print | grep boot | awk '{gsub(/[^[:digit:]]/, "", $2); print $2 }')
losetup -d /dev/loop0
losetup -o 0 /dev/loop0 /tmp/alpine_vol_tx.raw
dd if=/usr/share/syslinux/mbr.bin of=/dev/loop0
sync
losetup -d /dev/loop0
losetup -o ${OFFSET} /dev/loop0 /tmp/alpine_vol_tx.raw
mkfs.ext2 /dev/loop0
e2label /dev/loop0 rootfs
sync
mount /dev/loop0 /mnt

apk --arch x86_64 -X ${REPO}/alpine/v3.4/main/ -U --allow-untrusted --root /mnt --initdb add alpine-base jq dropbear netcat-openbsd lz4
apk --arch x86_64 -X ${REPO}/alpine/edge/community/ -U --allow-untrusted --root /mnt add pv

mkdir -p /mnt/boot
for file in vmlinuz-virtgrsec initramfs-virtgrsec modloop-virtgrsec System.map-virtgrsec; do
    isoinfo -R -x "/boot/${file}" -i /root/alpine-virt-x86_64.iso > "/mnt/boot/${file}"
done
cp /root/extlinux.conf /mnt/boot/extlinux.conf
cp /root/cloud-init /mnt/etc/init.d/cloud-init
cp /root/interfaces /mnt/etc/network/interfaces
cp /root/configure_interface.sh /mnt/root/configure_interface.sh
cp /root/zstd-1.0.0/zstd /mnt/usr/local/bin/
echo "http://dl-cdn.alpinelinux.org/alpine/v3.4/main" > /mnt/etc/apk/repositories

# Data collected using following script:
#for rl in sysinit boot default shutdown; do
#    for svc in $(rc-update show $rl | cut -d\| -f1); do
#        echo "ln -s /etc/init.d/${svc} /mnt/etc/runlevels/${rl}/${svc}"
#    done
#done
ln -s /etc/init.d/devfs /mnt/etc/runlevels/sysinit/devfs
ln -s /etc/init.d/dmesg /mnt/etc/runlevels/sysinit/dmesg
ln -s /etc/init.d/hwdrivers /mnt/etc/runlevels/sysinit/hwdrivers
ln -s /etc/init.d/mdev /mnt/etc/runlevels/sysinit/mdev
ln -s /etc/init.d/modloop /mnt/etc/runlevels/sysinit/modloop
ln -s /etc/init.d/bootmisc /mnt/etc/runlevels/boot/bootmisc
ln -s /etc/init.d/hostname /mnt/etc/runlevels/boot/hostname
ln -s /etc/init.d/hwclock /mnt/etc/runlevels/boot/hwclock
ln -s /etc/init.d/keymaps /mnt/etc/runlevels/boot/keymaps
ln -s /etc/init.d/modules /mnt/etc/runlevels/boot/modules
ln -s /etc/init.d/networking /mnt/etc/runlevels/boot/networking
ln -s /etc/init.d/swap /mnt/etc/runlevels/boot/swap
ln -s /etc/init.d/sysctl /mnt/etc/runlevels/boot/sysctl
ln -s /etc/init.d/syslog /mnt/etc/runlevels/boot/syslog
ln -s /etc/init.d/urandom /mnt/etc/runlevels/boot/urandom
ln -s /etc/init.d/acpid /mnt/etc/runlevels/default/acpid
ln -s /etc/init.d/crond /mnt/etc/runlevels/default/crond
ln -s /etc/init.d/dropbear /mnt/etc/runlevels/default/dropbear
ln -s /etc/init.d/local /mnt/etc/runlevels/default/local
ln -s /etc/init.d/killprocs /mnt/etc/runlevels/shutdown/killprocs
ln -s /etc/init.d/mount-ro /mnt/etc/runlevels/shutdown/mount-ro
ln -s /etc/init.d/savecache /mnt/etc/runlevels/shutdown/savecache

ln -s /etc/init.d/cloud-init /mnt/etc/runlevels/default/cloud-init

cat > /mnt/etc/local.d/configure_interface.start << EOF
#!/bin/ash

/bin/ash /root/configure_interface.sh &
EOF
chmod +x /mnt/etc/local.d/configure_interface.start

patch -p0 < /root/modloop.patch
mkdir /mnt/.modloop
ln -s /.modloop/modules /mnt/lib/modules

extlinux -i /mnt/boot

rm /mnt/var/cache/apk/*
dd if=/dev/zero of=/mnt/zerofile
rm /mnt/zerofile

umount /mnt
mkdir -p /result
qemu-img convert -c -f raw -O qcow2 -o compat=0.10 /tmp/alpine_vol_tx.raw /result/alpine_vol_tx.qcow2
