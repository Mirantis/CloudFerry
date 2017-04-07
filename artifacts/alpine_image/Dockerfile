FROM alpine

RUN apk add --update cdrkit qemu-img parted e2fsprogs e2fsprogs-extra syslinux alpine-sdk

ADD files/* /root/
ADD http://dl-cdn.alpinelinux.org/alpine/v3.4/releases/x86_64/alpine-virt-3.4.3-x86_64.iso /root/alpine-virt-x86_64.iso
ADD https://github.com/facebook/zstd/archive/v1.0.0.tar.gz /root/zstd-1.0.0.tar.gz

CMD /root/build-image.sh
