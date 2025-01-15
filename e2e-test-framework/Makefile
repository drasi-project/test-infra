.PHONY: default docker-build kind-load drasi-apply test lint-check push-latets-ghcr

CLUSTER_NAME ?= kind
IMAGE_PREFIX ?= drasi-project
DOCKER_TAG_VERSION ?= latest
DOCKERX_OPTS ?= --load --cache-to type=inline,mode=max

default: docker-build

docker-build:
	docker buildx build . -f test-service/Dockerfile -t $(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION) $(DOCKERX_OPTS)
	docker buildx build test-proxy -t $(IMAGE_PREFIX)/e2e-test-proxy:$(DOCKER_TAG_VERSION) $(DOCKERX_OPTS)

kind-load:
	kind load docker-image $(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION) --name $(CLUSTER_NAME)
	kind load docker-image $(IMAGE_PREFIX)/e2e-test-proxy:$(DOCKER_TAG_VERSION) --name $(CLUSTER_NAME)

drasi-apply:
	drasi apply -f test-source-provider/e2e-test-service-provider.yaml

test:
	cargo test

lint-check:
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings -W clippy::print_stdout -A unused -A clippy::module_inception -A clippy::ptr_arg -A clippy::type_complexity

push-latest-ghcr:
	docker tag $(IMAGE_PREFIX)/e2e-test-service:latest ghcr.io/$(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION)
	docker push ghcr.io/$(IMAGE_PREFIX)/e2e-test-service:$(DOCKER_TAG_VERSION)
	docker tag $(IMAGE_PREFIX)/e2e-test-proxy:latest ghcr.io/$(IMAGE_PREFIX)/e2e-test-proxy:$(DOCKER_TAG_VERSION)
	docker push ghcr.io/$(IMAGE_PREFIX)/e2e-test-proxy:$(DOCKER_TAG_VERSION)