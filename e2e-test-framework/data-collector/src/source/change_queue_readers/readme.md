# Source Change Queue

## Redis
This is what a record on the Source Change Redis Stream looks like:

```JSON
{
    "data": [
        {
            "op": "i",
            "payload": {
                "after": {
                    "id": "public:Message: 5",
                    "labels": [
                        "Message"
                    ],
                    "properties": {
                        "From": "David",
                        "Message": "I am Spartacus",
                        "MessageId": 5
                    }
                },
                "before": {},
                "source": {
                    "db": "hello-world",
                    "lsn": 26699728,
                    "table": "node",
                    "ts_ms": 1731284656886,
                    "ts_sec": 1731284656
                }
            },
            "ts_ms": 1731284658107
        }
    ],
    "datacontenttype": "application/json",
    "id": "ca1b090e-9ea7-4a2c-b835-c281ef625422",
    "pubsubname": "drasi-pubsub",
    "source": "hello-world-reactivator",
    "specversion": "1.0",
    "time": "2024-11-11T00: 24: 19Z",
    "topic": "hello-world-change",
    "traceid": "00-ca355dba05e1d2b9ed41c4cea80dca93-faa68b1f1eeacf94-01",
    "traceparent": "00-ca355dba05e1d2b9ed41c4cea80dca93-faa68b1f1eeacf94-01",
    "tracestate": "",
    "type": "com.dapr.event.sent"
}
```