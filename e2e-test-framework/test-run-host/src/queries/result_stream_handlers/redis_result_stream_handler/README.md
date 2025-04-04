# Example of Bootstrap sequence

```json
{
    "data": {
        "controlSignal": {
            "kind": "bootstrapStarted"
        },
        "kind": "control",
        "metadata": null,
        "queryId": "city-population",
        "sequence": 1,
        "sourceTimeMs": 1743741854398
    },
    "datacontenttype": "application/json",
    "id": "b5b887fc-63ab-441a-a269-72c2f5459ca0",
    "pubsubname": "drasi-pubsub",
    "source": "default-query-host",
    "specversion": "1.0",
    "time": "2025-04-04T04:44:14Z",
    "topic": "city-population-results",
    "traceid": "00-e33e2a1fc91f0d3f7e6f21faf2e7bd62-eb7aa1225549e495-01",
    "traceparent": "00-e33e2a1fc91f0d3f7e6f21faf2e7bd62-eb7aa1225549e495-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}

{
    "data": {
        "addedResults": [
            {
                "CityId": "Q473914",
                "CountryId": "Q29",
                "Name": "Ibiza",
                "Population": 51996
            }
        ],
        "deletedResults": [],
        "kind": "change",
        "metadata": null,
        "queryId": "city-population",
        "sequence": 2,
        "sourceTimeMs": 0,
        "updatedResults": []
    },
    "datacontenttype": "application/json",
    "id": "c386509b-17ca-48fa-8b31-9ce41091c490",
    "pubsubname": "drasi-pubsub",
    "source": "default-query-host",
    "specversion": "1.0",
    "time": "2025-04-04T04:44:14Z",
    "topic": "city-population-results",
    "traceid": "00-e33e2a1fc91f0d3f7e6f21faf2e7bd62-682c6a82037f0cf5-01",
    "traceparent": "00-e33e2a1fc91f0d3f7e6f21faf2e7bd62-682c6a82037f0cf5-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}

{
    "data": {
        "controlSignal": {
            "kind": "bootstrapCompleted"
        },
        "kind": "control",
        "metadata": null,
        "queryId": "city-population",
        "sequence": 4,
        "sourceTimeMs": 1743741854426
    },
    "datacontenttype": "application/json",
    "id": "99de1bb6-a435-4b4e-9e49-c8f48f815b2b",
    "pubsubname": "drasi-pubsub",
    "source": "default-query-host",
    "specversion": "1.0",
    "time": "2025-04-04T04:44:14Z",
    "topic": "city-population-results",
    "traceid": "00-e33e2a1fc91f0d3f7e6f21faf2e7bd62-c2ae50c30fdbb35e-01",
    "traceparent": "00-e33e2a1fc91f0d3f7e6f21faf2e7bd62-c2ae50c30fdbb35e-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}
```

# Example of Change sequence

```json
{
    "data": {
        "controlSignal": {
            "kind": "running"
        },
        "kind": "control",
        "metadata": null,
        "queryId": "city-population",
        "sequence": 5,
        "sourceTimeMs": 1743741854429
    },
    "datacontenttype": "application/json",
    "id": "f7fa5ac6-3251-438b-ace5-2be8a1729bcf",
    "pubsubname": "drasi-pubsub",
    "source": "default-query-host",
    "specversion": "1.0",
    "time": "2025-04-04T04:44:14Z",
    "topic": "city-population-results",
    "traceid": "00-9287e344a700d1b1b6b69efbb32d22a2-0f8d72e29181f45f-01",
    "traceparent": "00-9287e344a700d1b1b6b69efbb32d22a2-0f8d72e29181f45f-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}

{
    "data": {
        "addedResults": [],
        "deletedResults": [],
        "kind": "change",
        "metadata": {
            "tracking": {
                "query": {
                    "dequeue_ns": 1743741854432056426,
                    "enqueue_ns": 1743741854431764718,
                    "queryEnd_ns": 1743741854432584968,
                    "queryStart_ns": 1743741854432186926
                },
                "source": {
                    "changeDispatcherEnd_ns": 1743741854424872676,
                    "changeDispatcherStart_ns": 1743741854424758093,
                    "changeRouterEnd_ns": 1743741854423657885,
                    "changeRouterStart_ns": 1743741854423615510,
                    "reactivatorEnd_ns": 1743741854421592469,
                    "reactivatorStart_ns": 1743741854421592468,
                    "seq": 11126,
                    "source_ns": 1743741854421592468
                }
            }
        },
        "queryId": "city-population",
        "sequence": 6,
        "sourceTimeMs": 1743741854421,
        "updatedResults": [
            {
                "after": {
                    "CityId": "Q186182",
                    "CountryId": "Q668",
                    "Name": "Dharamshala",
                    "Population": 20034
                },
                "before": {
                    "CityId": "Q186182",
                    "CountryId": "Q668",
                    "Name": "Dharamshala",
                    "Population": 19034
                },
                "grouping_keys": null
            }
        ]
    },
    "datacontenttype": "application/json",
    "id": "55a0277d-3098-42df-85af-62746122de97",
    "pubsubname": "drasi-pubsub",
    "source": "default-query-host",
    "specversion": "1.0",
    "time": "2025-04-04T04:44:14Z",
    "topic": "city-population-results",
    "traceid": "00-191382015cde55eb4b79382759e1e723-f194ee5f96cbf1ae-01",
    "traceparent": "00-191382015cde55eb4b79382759e1e723-f194ee5f96cbf1ae-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}

{
    "data": {
        "addedResults": [],
        "deletedResults": [],
        "kind": "change",
        "metadata": {
            "tracking": {
                "query": {
                    "dequeue_ns": 1743741854433996010,
                    "enqueue_ns": 1743741854433783176,
                    "queryEnd_ns": 1743741854434472843,
                    "queryStart_ns": 1743741854434035760
                },
                "source": {
                    "changeDispatcherEnd_ns": 1743741854433375385,
                    "changeDispatcherStart_ns": 1743741854433346760,
                    "changeRouterEnd_ns": 1743741854424735218,
                    "changeRouterStart_ns": 1743741854424706593,
                    "reactivatorEnd_ns": 1743741854423608302,
                    "reactivatorStart_ns": 1743741854423608301,
                    "seq": 11359,
                    "source_ns": 1743741854423608301
                }
            }
        },
        "queryId": "city-population",
        "sequence": 7,
        "sourceTimeMs": 1743741854423,
        "updatedResults": [
            {
                "after": {
                    "CityId": "Q473914",
                    "CountryId": "Q29",
                    "Name": "Ibiza",
                    "Population": 52996
                },
                "before": {
                    "CityId": "Q473914",
                    "CountryId": "Q29",
                    "Name": "Ibiza",
                    "Population": 51996
                },
                "grouping_keys": null
            }
        ]
    },
    "datacontenttype": "application/json",
    "id": "376f642f-293f-4506-a480-fe8d329e2266",
    "pubsubname": "drasi-pubsub",
    "source": "default-query-host",
    "specversion": "1.0",
    "time": "2025-04-04T04:44:14Z",
    "topic": "city-population-results",
    "traceid": "00-6c4e0b2864ec25a744c8a37a28862968-c6390104448b3118-01",
    "traceparent": "00-6c4e0b2864ec25a744c8a37a28862968-c6390104448b3118-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}


```