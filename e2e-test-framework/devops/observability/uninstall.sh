#!/bin/bash

# Ensure Kind cluster is running
if ! kind get clusters | grep -q "kind"; then
  echo "No Kind cluster found. Nothing to clean up."
  exit 1
fi

# Delete deployments and associated resources from manifests
echo "Deleting resources from manifests..."
kubectl delete -f grafana.yaml --ignore-not-found
kubectl delete -f prometheus.yaml --ignore-not-found
kubectl delete -f otel-collector.yaml --ignore-not-found
kubectl delete -f tempo.yaml --ignore-not-found

echo "Cleanup complete!"