DOCKER_COMPOSE_CMD ?= docker compose
DOCKER_COMPOSE_ENV=--env-file .env

.PHONY: start
start:
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) up --force-recreate --remove-orphans --detach

.PHONY: stop
stop:
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) down --remove-orphans --volumes

.PHONY: restart
restart:
ifdef SERVICE
	service := $(SERVICE)
endif

ifdef service
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) stop $(service)
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) rm --force $(service)
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) create $(service)
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) start $(service)
else
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) down --remove-orphans --volumes
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) up --force-recreate --remove-orphans --detach
endif

.PHONY: redeploy
redeploy:
ifdef SERVICE
	service := $(SERVICE)
endif

ifdef service
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) build $(service)
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) stop $(service)
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) rm --force $(service)
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) create $(service)
	$(DOCKER_COMPOSE_CMD) $(DOCKER_COMPOSE_ENV) start $(service)
else
	@echo "Please provide a service name using `service=[service name]` or `SERVICE=[service name]`"
endif