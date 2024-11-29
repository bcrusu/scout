ARG DEBIAN_IMAGE=debian:bookworm-slim

FROM ${DEBIAN_IMAGE} AS rootfs_builder

COPY overlay /
COPY .work/id_rsa.pub /root/.ssh/authorized_keys

# Adapted from https://github.com/firecracker-microvm/firecracker/blob/main/resources/chroot.sh
RUN set -eux; \
    apt-get update && apt-get upgrade -y; \
    apt-get install -y --no-install-recommends \
        udev \
        systemd-sysv \
        openssh-server \
        socat; \
    echo "scout" > /etc/hostname; \
    passwd -d root; \
    mkdir -p /etc/systemd/system/sysinit.target.wants; \
    ln -s /etc/systemd/system/scout-setup.service /etc/systemd/system/sysinit.target.wants/scout-setup.service; \
    ln -s /usr/share/systemd/tmp.mount /etc/systemd/system/tmp.mount; \
    systemctl enable tmp.mount; \
    systemctl disable e2scrub_reap.service; \
    rm -vf /etc/systemd/system/timers.target.wants/*; \
    mkdir -p /old_root /workfs /scout;

# Dependencies required by Jepsen
# Listed at: https://github.com/jepsen-io/jepsen/blob/main/jepsen/src/jepsen/os/debian.clj#L165
RUN set -eux; \
    apt-get install -y --no-install-recommends \
        build-essential \
        bzip2 \
        ca-certificates \
        curl \
        dirmngr \
        dnsutils \
        faketime \
        iproute2 \
        iptables \
        iputils-ping \
        libzip4 \
        logrotate \
        lsb-release \
        man \
        man-db \
        net-tools \
        netcat-openbsd \
        ntpdate \
        psmisc \
        python3 \
        rsyslog \
        sudo \
        tcpdump \
        unzip \
        vim \
        wget; \
    rm -rf /var/lib/apt/lists /usr/share/doc /var/cache/apt/archives; 

FROM scratch AS scoutfs_builder

COPY --from=scout/control:latest /scout/control /control
COPY --from=scout/data:latest /scout/data /data
COPY --from=scout/api:latest /scout/api /api
