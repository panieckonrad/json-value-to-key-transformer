Kafka Connect SMT to for moving record value field (must be JSON string) to record key (String)

Useful for partitioning records coming from RABBITMQ queues which contain JSON strings. </br>
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`field`| json field which is supposed to be kafka record key | String | `"payload.country"` | High |

Example on how to add to your connector:
```
      "transforms":"jsonvaluetokey",
      "transforms.jsonvaluetokey.type":"com.kainos.smt.JsonValueToKey",
      "transforms.jsonvaluetokey.field":"payload.country"
```
