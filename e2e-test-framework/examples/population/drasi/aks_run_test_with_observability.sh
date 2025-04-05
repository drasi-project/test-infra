# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

GREEN="\033[32m"
RESET="\033[0m"

echo -e "${GREEN}\n\nConfigure observability stack...${RESET}"
# Delete pre-installed otel collector. This is a workaround.
kubectl delete deployment otel-collector -n drasi-system
kubectl delete svc otel-collector -n drasi-system
kubectl delete configmap otel-collector-conf -n drasi-system

# Deploy the new observability stack
# Deploy Tempo
kubectl apply -f ./devops/observability/tempo.yaml
echo "Waiting for Tempo..."
kubectl wait --for=condition=Ready pod -l app=tempo -n drasi-system --timeout=5m

# Deploy OTel Collector
kubectl apply -f ./devops/observability/otel-collector.yaml
echo "Waiting for OpenTelemetry Collector..."
kubectl wait --for=condition=Available deployment/otel-collector -n drasi-system --timeout=5m

# Deploy Prometheus
kubectl apply -f ./devops/observability/prometheus.yaml
echo "Waiting for Prometheus..."
kubectl wait --for=condition=Available deployment/prometheus -n drasi-system --timeout=5m

# Deploy Grafana
kubectl apply -f ./devops/observability/grafana.yaml
echo "Waiting for Grafana..."
kubectl wait --for=condition=Available deployment/grafana -n drasi-system --timeout=5m

# Deploy the Test Service and wait for it to be available
echo -e "${GREEN}\n\nDeploying Test Service...${RESET}"
kubectl apply -f examples/population/drasi/test_service_deployment.yaml
kubectl wait -n drasi-system --for=condition=available deployment/drasi-test-service --timeout=300s

# Install the Test Source Provider and create the Test Source
echo -e "${GREEN}\n\nRegistering E2ETestService SourceProvider with Drasi...${RESET}"
drasi apply -f ./devops/drasi/e2e_test_source_provider.yaml

echo -e "${GREEN}\n\nCreating Test Source...${RESET}"
drasi apply -f examples/population/drasi/source.yaml
drasi wait -f examples/population/drasi/source.yaml -t 200

# Create the Continuous Queries
echo -e "${GREEN}\n\nCreating Drasi Continuous Queries...${RESET}"
drasi apply -f examples/population/drasi/query.yaml
drasi wait -f examples/population/drasi/query.yaml -t 200

# Forward the Test Service port and configure the Repository, Source, and Query
echo -e "${GREEN}\n\nPort forwarding to enable access the Test Service Web API...${RESET}"
kubectl port-forward -n drasi-system services/drasi-test-service 63123:63123 &

echo -e "${GREEN}\n\nDeployment Complete.${RESET}"