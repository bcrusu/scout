CMDS=control data api admin testing
DOCKER_IMAGES=$(foreach CMD,$(CMDS),docker_image_$(CMD))

.PHONY: docker_images
docker_images: $(DOCKER_IMAGES)

.PHONY: $(DOCKER_IMAGES)
$(DOCKER_IMAGES):
	@set -eu; \
	CMD=$(subst docker_image_,,$@); \
	docker build --target scout --build-arg CMD_NAME=$$CMD -t scout/$$CMD .
