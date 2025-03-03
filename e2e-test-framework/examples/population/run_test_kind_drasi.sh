#!/bin/bash

GREEN="\033[32m"
RESET="\033[0m"

echo -e "${GREEN}\n\nSetting up Kind cluster...${RESET}"
kind create cluster

echo -e "${GREEN}\n\nInstalling Drasi...${RESET}"
drasi init
drasi init

echo -e "${GREEN}\n\nConfigure observability stack...${RESET}"
# Delete pre-installed otel collector. This is a workaround.
kubectl delete deployment otel-collector -n drasi-system
kubectl delete svc otel-collector -n drasi-system
kubectl delete configmap otel-collector-conf -n drasi-system

# Deploy the Test Service and wait for it to be available
echo -e "${GREEN}\n\nDeploying Test Service...${RESET}"
kubectl apply -f ./devops/test-service-deployment.yaml
kubectl wait -n drasi-system --for=condition=available deployment/drasi-test-service --timeout=300s

# Forward the Test Service port and configure the Repository, Source, and Query
echo -e "${GREEN}\n\nPort forwarding to enable access the Test Service Web API...${RESET}"
kubectl port-forward -n drasi-system services/drasi-test-service 63123:63123 &
sleep 5

echo -e "${GREEN}\n\nAdding Test Repository, Test Source, and Test Query...${RESET}"
curl -i -X POST -H "Content-Type: application/json" -d @examples/population/kind_drasi/cfg_repo_az_dev.json http://localhost:63123/test_repos
curl -i -X POST -H "Content-Type: application/json" -d @examples/population/kind_drasi/cfg_source_population.json http://localhost:63123/test_run_host/sources
curl -i -X POST -H "Content-Type: application/json" -d @examples/population/kind_drasi/cfg_query_city_population.json http://localhost:63123/test_run_host/queries

# Install the Test Source Provider and create the Test Source
echo -e "${GREEN}\n\nRegistering E2ETestService SourceProvider with Drasi...${RESET}"
drasi apply -f ./devops/e2e-test-source-provider.yaml

echo -e "${GREEN}\n\nCreating Test Source...${RESET}"
drasi apply -f examples/population/kind_drasi/source.yaml
drasi wait -f examples/population/kind_drasi/source.yaml -t 200

# Create the Continuous Queries
echo -e "${GREEN}\n\nCreating Drasi Continuous Queries...${RESET}"
drasi apply -f examples/population/kind_drasi/query.yaml
drasi wait -f examples/population/kind_drasi/query.yaml -t 200

# Start the Test Run Query
echo -e "${GREEN}\n\nStarting the Test Run Query...${RESET}"
curl -X POST -H "Content-Type: application/json" http://localhost:63123/test_run_host/queries/az_dev_repo.population.test_run_001.city-population/start

# Start the Test Run Source 
echo -e "${GREEN}\n\nStarting the Test Run Source...${RESET}"
curl -X POST -H "Content-Type: application/json" http://localhost:63123/test_run_host/sources/az_dev_repo.population.test_run_001.geo-db/start

echo -e "${GREEN}\n\nDeployment Complete.${RESET}"