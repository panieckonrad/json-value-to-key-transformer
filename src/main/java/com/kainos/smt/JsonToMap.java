package com.kainos.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.json.JSONObject;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class JsonToMap<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Transform json string into a Map";
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
        Map<String, ?> jsonMap = new JSONObject(record.value()).toMap();
        final Map<String, ?> valueJson = requireMap(jsonMap, PURPOSE);
        return newRecord(record, valueJson);
    }

    protected R newRecord(R record, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
    }
}

