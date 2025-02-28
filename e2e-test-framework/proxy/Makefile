.PHONY: default docker-build docker-tag-and-push kind-load test lint-check 

CLUSTER_NAME ?= kind
IMAGE_PREFIX ?= drasi-project
DOCKER_TAG_VERSION ?= latest
DOCKERX_OPTS ?= --load --cache-to type=inline,mode=max

default: docker-build

docker-build:
	docker buildx build .. -f Dockerfile -t $(IMAGE_PREFIX)/e2e-proxy:$(DOCKER_TAG_VERSION) $(DOCKERX_OPTS)

docker-tag-and-push:
	docker tag $(IMAGE_PREFIX)/e2e-proxy:latest ghcr.io/$(IMAGE_PREFIX)/e2e-proxy:$(DOCKER_TAG_VERSION)
	docker push ghcr.io/$(IMAGE_PREFIX)/e2e-proxy:$(DOCKER_TAG_VERSION)

kind-load:
	kind load docker-image $(IMAGE_PREFIX)/e2e-proxy:$(DOCKER_TAG_VERSION) --name $(CLUSTER_NAME)

test:
	cargo test