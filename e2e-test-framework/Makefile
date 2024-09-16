.PHONY: default docker-build kind-load

CLUSTER_NAME ?= kind
IMAGE_PREFIX ?= drasi-project
DOCKER_TAG_VERSION?=latest

default: docker-build

docker-build:
	docker build . -f test-runner-service/Dockerfile -t $(IMAGE_PREFIX)/e2e-test-runner:$(DOCKER_TAG_VERSION)
	docker build test-proxy -t $(IMAGE_PREFIX)/e2e-test-proxy:$(DOCKER_TAG_VERSION)


kind-load:
	kind load docker-image $(IMAGE_PREFIX)/e2e-test-runner:$(DOCKER_TAG_VERSION) --name $(CLUSTER_NAME)
	kind load docker-image $(IMAGE_PREFIX)/e2e-test-proxy:$(DOCKER_TAG_VERSION) --name $(CLUSTER_NAME)

drasi-apply:
	drasi apply -f test-source-provider/e2e-test-source-provider.yaml

test:
	echo "No tests to run yet"
