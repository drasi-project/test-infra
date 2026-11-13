.PHONY: default docker-build kind-load k3d-load test lint-check

IMAGE_PREFIX ?= drasi-project
DOCKER_TAG_VERSION ?= latest
TAG_SUFFIX ?=
DOCKERX_OPTS ?= --load --cache-to type=inline,mode=max
CONTAINER_BUILD ?= docker buildx build
DRASI_CORE_CONTEXT ?= ../../../drasi-core
BUILD_CONFIG ?= default

ifneq ($(BUILD_CONFIG), default)
	DOCKER_TAG_VERSION := ${DOCKER_TAG_VERSION}-$(BUILD_CONFIG)
endif

ifneq ($(TAG_SUFFIX),)
	DOCKER_TAG_VERSION := ${DOCKER_TAG_VERSION}$(TAG_SUFFIX)
endif

default: docker-build

docker-build:
	$(CONTAINER_BUILD) .. -f Dockerfile.$(BUILD_CONFIG) -t $(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION) \
		--build-context drasi-core=$(DRASI_CORE_CONTEXT) $(DOCKERX_OPTS)

kind-load:
	kind load docker-image $(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION) --name $(if $(CLUSTER_NAME),$(CLUSTER_NAME),kind)

k3d-load:
	k3d image import $(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION) -c $(if $(CLUSTER_NAME),$(CLUSTER_NAME),k3s-default)

test:
	cargo test

lint-check:
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings -W clippy::print_stdout