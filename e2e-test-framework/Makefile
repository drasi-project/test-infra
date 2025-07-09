.PHONY: default docker-build docker-tag-and-push kind-load drasi-apply test lint-check 

CLUSTER_NAME ?= kind
IMAGE_PREFIX ?= drasi-project
DOCKER_TAG_VERSION ?= latest
DOCKERX_OPTS ?= --load --cache-to type=inline,mode=max

default: docker-build

docker-build:
	docker buildx build . -f proxy/Dockerfile -t $(IMAGE_PREFIX)/e2e-proxy:$(DOCKER_TAG_VERSION) $(DOCKERX_OPTS)
	docker buildx build . -f reactivator/Dockerfile -t $(IMAGE_PREFIX)/e2e-reactivator:$(DOCKER_TAG_VERSION) $(DOCKERX_OPTS)
	docker buildx build . -f test-service/Dockerfile -t $(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION) $(DOCKERX_OPTS)

docker-tag-and-push:
	docker tag $(IMAGE_PREFIX)/e2e-proxy:latest ghcr.io/$(IMAGE_PREFIX)/e2e-proxy:$(DOCKER_TAG_VERSION)
	docker push ghcr.io/$(IMAGE_PREFIX)/e2e-proxy:$(DOCKER_TAG_VERSION)
	docker tag $(IMAGE_PREFIX)/e2e-reactivator:latest ghcr.io/$(IMAGE_PREFIX)/e2e-reactivator:$(DOCKER_TAG_VERSION)
	docker push ghcr.io/$(IMAGE_PREFIX)/e2e-reactivator:$(DOCKER_TAG_VERSION)
	docker tag $(IMAGE_PREFIX)/e2e-test-service:latest ghcr.io/$(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION)
	docker push ghcr.io/$(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION)

kind-load:
	kind load docker-image $(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION) --name $(CLUSTER_NAME)
	kind load docker-image $(IMAGE_PREFIX)/e2e-test-proxy:$(DOCKER_TAG_VERSION) --name $(CLUSTER_NAME)

drasi-apply:
	drasi apply -f test-source-provider/e2e-test-service-provider.yaml

lint-check:
	$(MAKE) -C test-service lint-check
	$(MAKE) -C reactivator lint-check
	$(MAKE) -C proxy lint-check

test:
	cargo test