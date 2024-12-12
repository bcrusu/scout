# syntax=docker/dockerfile:1

ARG DEBIAN_IMAGE=debian:bookworm-slim

FROM ${DEBIAN_IMAGE} AS rootfs_builder
ARG SCOUT_UID="888"
ARG SCOUT_GID="888"

COPY overlay /
COPY .work/id_rsa.pub /root/.ssh/authorized_keys

# Adapted from https://github.com/firecracker-microvm/firecracker/blob/main/resources/chroot.sh
RUN set -eux; \
    apt-get update && apt-get upgrade -y; \
    apt-get install -y --no-install-recommends \
        udev \
        systemd-sysv \
        openssh-server \
        iproute2; \
    echo "scout" > /etc/hostname; \
    passwd -d root; \
    systemctl enable \
        tmp.mount \
        scout-setup.service \
        scout-agent.service; \
    systemctl disable e2scrub_reap.service; \
    rm -vf /etc/systemd/system/timers.target.wants/*; \
    mkdir -p /old_root /workfs /scout; \
    addgroup --system --gid ${SCOUT_GID} scout; \
    adduser --system --uid ${SCOUT_UID} --ingroup scout scout;

FROM scratch AS scoutfs_builder

COPY --from=scout/control:latest /scout/control /control
COPY --from=scout/data:latest /scout/data /data
COPY --from=scout/api:latest /scout/api /api
COPY --from=scout/testing:latest /scout/testing /testing
