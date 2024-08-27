# Building

To create the docker images for the ETF, from the e2e-test-framework folder, run make:

```
make
```

This will create the following docker images and push them to the local docker repo:
- drasi-project/e2e-test-runner:latest
- drasi-project/e2e-test-proxy:latest

To see that the images are in the local repo, run:

```
docker image list
```

Then if developing locally using Kind, need to add the images to the kind cluster:

```
make kind-load
```

To check that the images are on the Kind cluster run:

```
docker exec -it $(kind get clusters | head -1)-control-plane crictl images
```

You will see them listed as:
- docker.io/drasi-project/e2e-test-runner:latest
- docker.io/drasi-project/e2e-test-proxy:latest


Install the E2ETestSource SourceProvider definition to Drasi:

```
make drasi-apply
```

To check, run:

```
drasi list sourceprovider
```

You will see output like the following:

```
       ID
-----------------
  PostgreSQL
  SQLServer
  CosmosGremlin
  E2ETestSource
```


# Using ETF
You can now create instances of E2ETestSource to use for testing.


Create a E2ETestSource Source definition file. See test-source/facilities-test-source.yaml as an example.

Apply the E2ETestSource Source definition to Drasi:

```
drasi apply -f test-source/e2e-test-source-facilities.yaml
```

To check, run:

```
drasi list source
```

You will see output like this:

```
              ID             | AVAILABLE
-----------------------------+------------
  e2e-test-source-facilities | true
```

# Connecting

## test-runner

Forward the api port of the test-runner, which is filling the slot of the reactivator:

```
kubectl port-forward -n drasi-system services/e2e-test-source-facilities-api 5000:5000
```

Browse to the following address:

```
http://localhost:5000/
```

You will see:

```
{
    "service_status": "Ready",
    "local_test_repo": {
        "data_cache_path": "./source_data_cache",
        "data_sets": []
    },
    "reactivators": []
}
```


## test-proxy


# Cleanup

Stop forwarded ports.

Delete the E2ETestSource

```
drasi delete source e2e-test-source-facilities
```

Delete the E2EtestSourceProvider

```
drasi delete sourceprovider E2ETestSource
```

