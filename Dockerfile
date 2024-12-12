# syntax=docker/dockerfile:1

ARG BASE_IMAGE=busybox:1.37.0-glibc
ARG DEBIAN_IMAGE=debian:bookworm
ARG GOLANG_IMAGE=golang:1.23.3-bookworm

###################################################################
# RocksDB builder
###################################################################
FROM ${DEBIAN_IMAGE} AS rocksdb_builder
ARG J=7
WORKDIR /rocksdb/

# Dependencies:
RUN set -eux; \
    apt-get update && apt-get upgrade -y; \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        make \
        gcc \
        g++ \
        libsnappy-dev \
        zlib1g-dev \
        libbz2-dev \
        liblz4-dev \
        libzstd-dev;

# Build my fork:
RUN --mount=type=cache,target=/rocksdb/cache/ \
    set -eux; \
    apt-get install -y --no-install-recommends git; \
    if [ -d "/rocksdb/cache/.git/" ]; then \
        cd cache; \
        git pull; \
    else \
        git clone -b misc_fixes --depth 1 https://github.com/bcrusu/rocksdb.git cache; \
        cd cache; \
    fi; \
    make install-static -j${J};

# Build latest release:
# RUN set -eux; \
#     rocksdb_url="https://github.com/facebook/rocksdb"; \
#     latest=$(basename $(curl -fsSLI -o /dev/null -w %{url_effective} ${rocksdb_url}/releases/latest)); \
#     mkdir src; \
#     curl -L ${rocksdb_url}/archive/refs/tags/${latest}.tar.gz | tar -xz -C ./src --strip-components=1; \
#     cd src; \
#     make install-static -j${J};

###################################################################
# Scout builder
###################################################################
FROM ${GOLANG_IMAGE} AS scout_builder
ARG CMD_NAME=unset
ARG CGO_LDFLAGS="-static -s -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -ldl"
ARG LD_FLAGS="-linkmode 'external'"

WORKDIR /scout

COPY --from=rocksdb_builder /usr/lib/x86_64-linux-gnu/libz.a /usr/lib/x86_64-linux-gnu/
COPY --from=rocksdb_builder /usr/lib/x86_64-linux-gnu/libbz2.a /usr/lib/x86_64-linux-gnu/
COPY --from=rocksdb_builder /usr/lib/x86_64-linux-gnu/libsnappy.a /usr/lib/x86_64-linux-gnu/
COPY --from=rocksdb_builder /usr/lib/x86_64-linux-gnu/liblz4.a /usr/lib/x86_64-linux-gnu/
COPY --from=rocksdb_builder /usr/lib/x86_64-linux-gnu/libzstd.a /usr/lib/x86_64-linux-gnu/
COPY --from=rocksdb_builder /usr/local/lib/librocksdb.a /usr/local/lib/
COPY --from=rocksdb_builder /usr/local/include/rocksdb /usr/local/include/rocksdb

RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=cache,target=/root/.cache/go-build/ \
    --mount=type=bind,source=./,target=./src \
    set -eux; \
    cd src; \
    go mod download; \
    go install -ldflags "$LD_FLAGS" github.com/bcrusu/scout/cmd/${CMD_NAME};

###################################################################
# Scout image
###################################################################
FROM ${BASE_IMAGE} AS scout
ARG CMD_NAME=unset
ARG SCOUT_UID="888"
ARG SCOUT_GID="888"
ARG SCOUT_DIR="/scout"
ENV PATH="$PATH:/scout/"

WORKDIR ${SCOUT_DIR}

RUN set -eux; \
    mkdir -p ${SCOUT_DIR}; \
    addgroup --system --gid $SCOUT_GID scout; \
    adduser --system --uid $SCOUT_UID --ingroup scout scout; \
    chown -R scout:scout ${SCOUT_DIR}; \
    chmod -R 777 ${SCOUT_DIR};

USER ${SCOUT_UID}

# RPC port
EXPOSE 11001
# HTTP port
EXPOSE 8080

COPY --chmod=755 <<EOF /scout/entrypoint.sh
#!/bin/sh
exec /scout/${CMD_NAME} \$@
EOF

COPY --from=scout_builder /go/bin/${CMD_NAME} ${SCOUT_DIR}
ENTRYPOINT [ "/scout/entrypoint.sh" ]
