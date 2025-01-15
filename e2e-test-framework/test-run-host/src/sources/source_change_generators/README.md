# Example of Change Queue Message

```json
{
    "data": [
        {
            "op": "i",
            "payload": {
                "after": {
                    "nodeLabels": [
                        "Continent",
                        "Country"
                    ],
                    "queryId": "continent-country-population",
                    "queryNodeId": "default",
                    "relLabels": []
                },
                "before": null,
                "source": {
                    "db": "Drasi",
                    "table": "SourceSubscription"
                }
            },
            "ts_ms": 1736689880389
        }
    ],
    "datacontenttype": "application/json",
    "id": "5d9586f2-04ec-4d85-8580-dcd120c53128",
    "pubsubname": "drasi-pubsub",
    "source": "geo-db-query-api",
    "specversion": "1.0",
    "time": "2025-01-12T13:51:20Z",
    "topic": "geo-db-change",
    "traceid": "00-81d290797717a2f2bf229304654cbb46-1cd946f2160aca79-01",
    "traceparent": "00-81d290797717a2f2bf229304654cbb46-1cd946f2160aca79-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}
```