LOCAL_PORT=63123

PID=$(ps aux | grep "[k]ubectl port-forward.*$LOCAL_PORT" | awk '{print $2}')

if [ -n "$PID" ]; then
    echo "Found kubectl port-forward process with PID: $PID"
    # Kill the process
    kill -9 "$PID"
    if [ $? -eq 0 ]; then
        echo "Successfully stopped port forwarding on port $LOCAL_PORT"
    else
        echo "Failed to stop port forwarding"
    fi
else
    echo "No kubectl port-forward process found for port $LOCAL_PORT"
fi

# SHORTCUT FOR NOW
drasi uninstall -y
kubectl wait --for=delete namespace/drasi-system --timeout=5m
exit


drasi delete query continent-country-population
drasi delete query country-city-population
drasi delete query city-population

drasi delete source geo-db

drasi delete sourceprovider E2ETestSource

# Delete Test Service
kubectl delete service drasi-test-service -n drasi-system
kubectl delete deployment drasi-test-service -n drasi-system

# Delete Redis
kubectl delete statefulset drasi-redis -n drasi-system
kubectl delete pvc data-drasi-redis-0 -n drasi-system

# Delete Mongo
kubectl delete statefulset drasi-mongo -n drasi-system
kubectl delete pvc data-drasi-mongo-0 -n drasi-system
kubectl delete configmap drasi-mongo-init -n drasi-system