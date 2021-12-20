package com.kainos.smt;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.kainos.smt.TypeSafetyHelper.requireString;

public class JsonValueToKey<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Extract json field and insert it into record key";
    public static final String PURPOSE = "extracting json field and inserting it into record key";

    private interface ConfigName {
        String FIELD = "field";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD, ConfigDef.Type.STRING, "payload.country", ConfigDef.Importance.HIGH,
                    "which json field to move as key (in string format)");
    private String jsonField;


    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        jsonField = config.getString(ConfigName.FIELD);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public R apply(R record) {
        String value = requireString(record.value(), PURPOSE);
        ReadContext ctx = JsonPath.parse(value);
        Object updatedKey = Optional.ofNullable(ctx.read("$." + jsonField))
                .orElseThrow(() -> new NoSuchElementException("Element not found: " + jsonField));
        updatedKey = requireString(updatedKey, PURPOSE);

        return newRecord(record, updatedKey);
    }

    protected R newRecord(R record, Object updatedKey) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedKey, record.valueSchema(), record.value(), record.timestamp());
    }
}

