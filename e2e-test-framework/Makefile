.PHONY: default docker-build kind-load k3d-load test lint-check drasi-apply

default: docker-build

docker-build:
	$(MAKE) -C proxy $(MAKECMDGOALS)
	$(MAKE) -C reactivator $(MAKECMDGOALS)
	$(MAKE) -C test-service $(MAKECMDGOALS)

kind-load:
	$(MAKE) -C proxy $(MAKECMDGOALS)
	$(MAKE) -C reactivator $(MAKECMDGOALS)
	$(MAKE) -C test-service $(MAKECMDGOALS)

k3d-load:
	$(MAKE) -C proxy $(MAKECMDGOALS)
	$(MAKE) -C reactivator $(MAKECMDGOALS)
	$(MAKE) -C test-service $(MAKECMDGOALS)

test:
	$(MAKE) -C proxy $(MAKECMDGOALS)
	$(MAKE) -C reactivator $(MAKECMDGOALS)
	$(MAKE) -C test-service $(MAKECMDGOALS)

lint-check:
	$(MAKE) -C proxy $(MAKECMDGOALS)
	$(MAKE) -C reactivator $(MAKECMDGOALS)
	$(MAKE) -C test-service $(MAKECMDGOALS)

drasi-apply:
	drasi apply -f test-source-provider/e2e-test-service-provider.yaml