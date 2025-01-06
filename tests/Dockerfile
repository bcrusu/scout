# syntax=docker/dockerfile:1

ARG DEBIAN_IMAGE=debian:bookworm-slim

###################################################################
# RootFS builder
###################################################################
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
        dbus \
        systemd-sysv \
        openssh-server \
        iputils-ping \
        nftables \
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

###################################################################
# ScoutFS builder
###################################################################
FROM scratch AS scoutfs_builder

COPY --from=scout/control:latest /scout/control /control
COPY --from=scout/data:latest /scout/data /data
COPY --from=scout/api:latest /scout/api /api
COPY --from=scout/testing:latest /scout/testing /testing

###################################################################
# Elle CLI tool
###################################################################
FROM clojure:lein-bookworm-slim AS elle_cli
WORKDIR /elle

RUN set -eux; \
    apt-get update && apt-get upgrade -y; \
    apt-get install -y --no-install-recommends git graphviz; \
    git clone --depth=1 https://github.com/ligurio/elle-cli; \
    cd elle-cli; \
    lein deps; \
    mv "$(lein uberjar | sed -n 's/^Created \(.*standalone\.jar\)/\1/p')" /elle/elle-cli.jar;

COPY --chmod=755 <<EOF /elle/entrypoint.sh
#!/bin/sh
exec java -jar /elle/elle-cli.jar \$@
EOF

ENTRYPOINT [ "/elle/entrypoint.sh" ]
