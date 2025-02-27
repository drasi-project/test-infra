#!/bin/bash

NAMESPACE="drasi-system"
APP_NAME="drasi-test-service"
CONTAINER="drasi-test-service"

# Check if both parameters are provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <test-run-id>"
    echo "Example: $0 1"
    exit 1
fi

# Store the parameters
TEST_RUN_ID=$1
mkdir ${TEST_RUN_ID}

# Dynamically get the Pod name based on the app label
POD_NAME=$(kubectl get pod -n "$NAMESPACE" -l app="$APP_NAME" -o jsonpath="{.items[0].metadata.name}")

# Check if POD_NAME is empty (no matching Pod found)
if [ -z "$POD_NAME" ]; then
  echo "Error: No Pod found with label app=$APP_NAME in namespace $NAMESPACE"
  exit 1
fi

# Source
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/az_dev_repo.population.test_run_001/sources/geo-db/test_run_summary.json" "./${TEST_RUN_ID}/source_summary.json"

# Query Observer
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/az_dev_repo.population.test_run_001/queries/city-population/test_run_summary.json" "./${TEST_RUN_ID}/query_summary.json"

# Query Result Profiler
kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/az_dev_repo.population.test_run_001/queries/city-population/result_stream_log/profiler/test_run_summary.json" "./${TEST_RUN_ID}/query_profiler_summary.json"

kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/az_dev_repo.population.test_run_001/queries/city-population/result_stream_log/profiler/change_all_abs.png" "./${TEST_RUN_ID}/query_profiler_viz_all_abs.png"

kubectl cp -c "$CONTAINER" "$NAMESPACE/$POD_NAME:/drasi_data_store/test_runs/az_dev_repo.population.test_run_001/queries/city-population/result_stream_log/profiler/change_all_rel.png" "./${TEST_RUN_ID}/query_profiler_viz_all_rel.png"


