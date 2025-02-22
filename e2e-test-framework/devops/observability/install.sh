#!/bin/bash

# Ensure Kind cluster is running
if ! kind get clusters | grep -q "kind"; then
  echo "No Kind cluster found. Please create one with 'kind create cluster'."
  exit 1
fi

# Create Namespace
kubectl apply -f namespace.yaml

# Deploy Tempo
kubectl apply -f tempo.yaml
echo "Waiting for Tempo..."
kubectl wait --for=condition=Ready pod -l app=tempo -n drasi-system --timeout=5m

# Deploy OTel Collector
kubectl apply -f otel-collector.yaml
echo "Waiting for OpenTelemetry Collector..."
kubectl wait --for=condition=Available deployment/otel-collector -n drasi-system --timeout=5m

# Deploy Prometheus
kubectl apply -f prometheus.yaml
echo "Waiting for Prometheus..."
kubectl wait --for=condition=Available deployment/prometheus -n drasi-system --timeout=5m

# Deploy Grafana
kubectl apply -f grafana.yaml
echo "Waiting for Grafana..."
kubectl wait --for=condition=Available deployment/grafana -n drasi-system --timeout=5m

echo "Deployment complete! Access Grafana at http://localhost:3000 after port-forwarding."
echo "Run: kubectl port-forward svc/grafana 3000:3000 -n drasi-system"