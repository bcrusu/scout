ARCH=x86_64
WORK=$(PWD)/.work
DOWNLOADS=$(WORK)/downloads
VMLINUX=vmlinux-6.1.102
FC_VER=v1.10.1
FC_BIN=firecracker-$(FC_VER)-$(ARCH)
FC_TAR=$(FC_BIN).tgz
CNI_VER=v1.6.0
CNI_TAR=cni-plugins-linux-amd64-$(CNI_VER).tgz

all: dirs testing $(WORK)/firecracker fs cni $(WORK)/configs

.PHONY: dirs
dirs:
	@mkdir -p $(DOWNLOADS)

$(WORK)/id_rsa:
	cd $(WORK) && ssh-keygen -f id_rsa -N "";

$(WORK)/configs:
	ln -s $(PWD)/configs $(WORK)/configs

.PHONY: testing
testing:
	@set -eux; \
	$(MAKE) -C ../ docker_image_testing; \
	CONTAINER_ID=$$(docker create "scout/testing:latest"); \
	docker cp "$$CONTAINER_ID:/scout/testing" "$(WORK)/testing"; \
	docker rm "$$CONTAINER_ID";

########################################################
# Downloads
########################################################

$(DOWNLOADS)/$(VMLINUX):
	curl -L "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/x86_64/$(VMLINUX)" -o $(DOWNLOADS)/$(VMLINUX)

$(DOWNLOADS)/$(CNI_TAR):
	curl -L https://github.com/containernetworking/plugins/releases/download/$(CNI_VER)/$(CNI_TAR) -o $(DOWNLOADS)/$(CNI_TAR)

$(DOWNLOADS)/tc-redirect-tap:
	@set -eux; \
	cd $(DOWNLOADS); \
	git clone --depth 1 https://github.com/awslabs/tc-redirect-tap.git tc-redirect-tap;

$(WORK)/firecracker:
	@set -eux; \
	curl -L https://github.com/firecracker-microvm/firecracker/releases/download/$(FC_VER)/$(FC_TAR) -o $(DOWNLOADS)/$(FC_TAR); \
	cd $(WORK); \
	mkdir firecracker_tmp; \
	tar -xzf $(DOWNLOADS)/$(FC_TAR) -C firecracker_tmp --strip-components=1 --touch; \
	mv firecracker_tmp/$(FC_BIN) firecracker; \
	rm -rf firecracker_tmp;

########################################################
# ext4 filesystems
########################################################

fs: $(WORK)/rootfs.ext4 $(WORK)/scoutfs.ext4 $(WORK)/workfs.ext4

$(WORK)/rootfs.ext4: $(WORK)/id_rsa
	@set -eux; \
    truncate -s 1000M $(WORK)/rootfs.ext4; \
	docker build --target rootfs_builder --output type=tar,dest=- . | mkfs.ext4 -F $(WORK)/rootfs.ext4 -d -;

$(WORK)/scoutfs.ext4:
	@set -eux; \
    truncate -s 200M $(WORK)/scoutfs.ext4; \
	$(MAKE) -C ../ docker_images; \
	docker build --target scoutfs_builder --output type=tar,dest=- . | mkfs.ext4 -F $(WORK)/scoutfs.ext4 -d -;

$(WORK)/workfs.ext4:
	@set -eux; \
	cd $(WORK); \
    truncate -s 1024M workfs.ext4; \
    mkfs.ext4 -F workfs.ext4;

########################################################
# CNI
########################################################

cni: $(WORK)/cni_bin $(WORK)/cni_bin/tc-redirect-tap $(WORK)/cni_conf

$(WORK)/cni_bin: $(DOWNLOADS)/$(CNI_TAR)
	@set -eux; \
	cd $(WORK); \
	mkdir cni_bin; \
	tar -xzf $(DOWNLOADS)/$(CNI_TAR) -C cni_bin --touch;

$(WORK)/cni_bin/tc-redirect-tap: $(DOWNLOADS)/tc-redirect-tap
	@set -eux; \
	echo $(WORK)/cni_bin/tc-redirect-tap; \
	cd $(DOWNLOADS)/tc-redirect-tap; \
	go build -o $(WORK)/cni_bin/ github.com/awslabs/tc-redirect-tap/cmd/tc-redirect-tap;

$(WORK)/cni_conf:
	ln -s $(PWD)/cni_conf $(WORK)/cni_conf

########################################################
# Some quick smoke tests:
########################################################

TAP_DEV="tap888"
TAP_IP="172.16.0.1"
MASK_SHORT="/30"
FC_CONFIG=fc.config
FC_SOCKET=/tmp/firecracker.socket
FC_LOG=/tmp/firecracker.log
FC_WORKFS=/tmp/firecracker.ext4

setup_net:
	@set -eux; \
	sudo sh -c "echo 1 > /proc/sys/net/ipv4/ip_forward"; \
	sudo ip link del "$(TAP_DEV)" 2> /dev/null || true; \
	sudo ip tuntap add dev "$(TAP_DEV)" mode tap; \
	sudo ip addr add "$(TAP_IP)$(MASK_SHORT)" dev "$(TAP_DEV)"; \
	sudo ip link set dev "$(TAP_DEV)" up;

start_fc: fs
	@set -eux; \
	sudo rm -f $(FC_LOG) $(FC_SOCKET) $(FC_WORKFS); \
	sudo touch $(FC_LOG); \
	sudo cp $(WORK)/workfs.ext4 $(FC_WORKFS); \
	sudo $(WORK)/firecracker --api-sock $(FC_SOCKET) --config-file $(FC_CONFIG);
