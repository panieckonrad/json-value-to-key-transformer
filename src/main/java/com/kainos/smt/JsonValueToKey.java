package com.kainos.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.json.JSONObject;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class JsonValueToKey<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Extract json field and insert it into record key";
    private static final String PURPOSE = "transforming json string into map";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public void configure(Map<String, ?> props) {
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
        String value = (String) record.value();
        JSONObject json = new JSONObject(value);
        Object updatedKey = Optional.ofNullable(json.getJSONObject("payload").getString("country"))
                .orElseThrow(() -> new NoSuchElementException("Element not found"));

        return newRecord(record, updatedKey);
    }

    protected R newRecord(R record, Object updatedKey) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedKey, record.valueSchema(), record.value(), record.timestamp());
    }
}

