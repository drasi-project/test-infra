# Running the City Population Demo

## Requirements

Need the Drasi CLI installed.
https://drasi.io/reference/command-line-interface/

Do the following steps from the ```test-infra/e2e-test-framework``` folder ...

## Install Drasi on a Kind Cluster

The following command will:

1. Setup a kind cluster
1. Install Drasi on the Kind cluster (https://drasi.io/how-to-guides/installation/install-on-kind/)
1. Register the E2ETestService SourceProvider with Drasi

## Setup Test Service
Create a new E2ETestService Source:

```
drasi apply -f ./test-service/tests/interactive/city_populations/source.yaml
```

Confirm the creation of the Test Service Source:

```
drasi list source
```

Look for the following:

```
    ID   | AVAILABLE | MESSAGES
---------+-----------+-----------
  geo-db | true      |
```

Forward the port so you can use the Test Service Web API:

```
kubectl port-forward -n drasi-system services/geo-db-api 5000:5000 &
```

Connext to the Test Service Web API:

http://localhost:5000/

## Configure the Test Service

Add the Azure Dev Repo:

```
curl -i -X POST -H "Content-Type: application/json" -d @test-service/tests/interactive/city_populations/add_az_dev_repo.json http://localhost:5000/test_repos
```

View the Azure Test Repo:

http://localhost:5000/test_repos/az_dev_repo


Add the Test Run Source for geo-db:

```
curl -i -X POST -H "Content-Type: application/json" -d @test-service/tests/interactive/city_populations/add_test_source.json http://localhost:5000/test_runner/sources
```

View the Test Run Source for geo-db:

http://localhost:5000/test_runner/sources/az_dev_repo.city_populations.test_run_001.geo-db


View bootstrap data:

```
curl -i -X POST -H "Content-Type: application/json" -d @test-service/tests/interactive/city_populations/bootstrap_query.json http://localhost:5000/acquire
```

## Configure the Continuous Queries

Create the queries:

```
drasi apply -f test-service/tests/interactive/city_populations/query.yaml
```

Confirm the queries:

```
drasi list query
```

See the following:

```
               ID              | CONTAINER | ERRORMESSAGE |              HOSTNAME               | STATUS
-------------------------------+-----------+--------------+-------------------------------------+----------
  continent-country-population | default   |              | default-query-host-5d757dd947-jqsr6 | Running
  country-city-population      | default   |              | default-query-host-5d757dd947-jqsr6 | Running
  city-population              | default   |              | default-query-host-5d757dd947-jqsr6 | Running
```

Watch the query:

```
drasi watch continent-country-population

drasi watch country-city-population

drasi watch city-population
```

## Configure Zipkin

```
kubectl create deployment zipkin --image openzipkin/zipkin
kubectl expose deployment zipkin --type ClusterIP --port 9411
kubectl port-forward svc/zipkin 9411:9411
http://localhost:9411 
```

## Control the Test Service Source
Use commands in ```kind_city_populations.http``` or the following curl commands:

### Start
curl -X POST -H "Content-Type: application/json" http://localhost:5000/test_runner/sources/az_dev_repo.city_populations.test_run_001.geo-db/start

### Pause
curl -X POST -H "Content-Type: application/json" http://localhost:5000/test_runner/sources/az_dev_repo.city_populations.test_run_001.geo-db/pause

### Step
curl -X POST -H "Content-Type: application/json" -d '{ "num_steps": 1 }' http://localhost:5000/test_runner/sources/az_dev_repo.city_populations.test_run_001.geo-db/step


### Stop
curl -X POST -H "Content-Type: application/json" http://localhost:5000/test_runner/sources/az_dev_repo.city_populations.test_run_001.geo-db/stop


## Cleanup

Remove demo drasi resources:

```
./test-service/tests/interactive/city_populations/cleanup
```

Delete the kind cluster:

```
kind delete cluster
```