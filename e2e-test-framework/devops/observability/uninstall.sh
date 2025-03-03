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