# Example of Result Strean Control Message

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
        "sourceTimeMs": 1736689880448
    },
    "datacontenttype": "application/json",
    "id": "468c370d-7e69-4d1b-a017-ebeb93b45675",
    "pubsubname": "drasi-pubsub",
    "source": "default-query-host",
    "specversion": "1.0",
    "time": "2025-01-12T13:51:20Z",
    "topic": "city-population-results",
    "traceid": "00-8419be50ea785e415249b3699c82b9de-527ad358232d8339-01",
    "traceparent": "00-8419be50ea785e415249b3699c82b9de-527ad358232d8339-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}
```

# Example of Result Stream Data Message

```json
{
    "data": {
        "addedResults": [
            {
                "CityId": "Q395201",
                "CountryId": "Q117",
                "Name": "Agogo",
                "Population": null
            }
        ],
        "deletedResults": [],
        "kind": "change",
        "metadata": null,
        "queryId": "city-population",
        "sequence": 3,
        "sourceTimeMs": 0,
        "updatedResults": []
    },
    "datacontenttype": "application/json",
    "id": "0a31bf42-e519-4fd5-800d-70492202de9a",
    "pubsubname": "drasi-pubsub",
    "source": "default-query-host",
    "specversion": "1.0",
    "time": "2025-01-13T16:01:24Z",
    "topic": "city-population-results",
    "traceid": "00-2665720837e0f88e8c6bbd6097d051b4-7e2e4f7793c96130-01",
    "traceparent": "00-2665720837e0f88e8c6bbd6097d051b4-7e2e4f7793c96130-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}
```