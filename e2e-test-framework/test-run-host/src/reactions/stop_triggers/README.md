# Reaction Stop Triggers

Stop triggers define conditions that automatically stop a reaction observer during test execution. They follow the same pattern as query stop triggers but are tailored for reaction-specific metrics.

## Available Stop Triggers

### Record Count
Stops the reaction observer when a specified number of reaction invocations have been processed.

**Configuration:**
```json
{
  "kind": "RecordCount",
  "record_count": 100
}
```

The trigger fires when `reaction_invocation_count >= record_count`.

### Record Sequence Number (Not Applicable)
While this trigger type exists in the configuration model for consistency with queries, it is not applicable to reactions. If configured, it will be converted to a trigger that never fires.

## Usage Example

In your test configuration, add stop triggers to reactions:

```json
{
  "reactions": [
    {
      "test_reaction_id": "my-reaction",
      "output_handler": {
        "kind": "Http",
        "port": 9001
      },
      "stop_triggers": [
        {
          "kind": "RecordCount",
          "record_count": 50
        }
      ]
    }
  ]
}
```

## Multiple Stop Triggers

When multiple stop triggers are configured, they are evaluated with OR logic - the reaction observer stops when ANY trigger condition is met.

## Metrics Used

Stop triggers evaluate conditions based on `ReactionObserverMetrics`:
- `reaction_invocation_count`: Total number of reaction invocations processed
- `observer_start_time_ns`: When the observer started
- `observer_stop_time_ns`: When the observer stopped
- `reaction_invocation_first_ns`: Timestamp of first invocation
- `reaction_invocation_last_ns`: Timestamp of last invocation

## Custom Stop Triggers

To implement a custom stop trigger:

1. Create a new struct implementing the `StopTrigger` trait
2. Add the configuration variant to `StopTriggerDefinition` 
3. Update the `create_stop_trigger` factory function
4. Implement the `is_true` method to evaluate your condition