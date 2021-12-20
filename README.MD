Kafka Connect SMT for moving record value field (must be JSON string) to record key.

Useful for partitioning records coming from RABBITMQ queues which contain JSON strings. </br>
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`fields`| json fields which are supposed to be in kafka record key | String | None | High |

Example on how to add to your connector:
```
        "transforms":"jsonValueToKey",
        "transforms.jsonValueToKey.type":"com.kainos.smt.JsonValueToKey",
        "transforms.jsonValueToKey.fields":"payload.country,producer"
```

Copy 'json-value-to-key-1.0-SNAPSHOT-jar-with-dependencies.jar' to your kafka connect cluster.
