package com.kainos.smt;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;

import static com.kainos.smt.TypeSafetyHelper.requireString;

public class JsonValueToKey<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Extract json field and insert it into record key";
    public static final String PURPOSE = "extracting json field and inserting it into record key";

    private interface ConfigName {
        String FIELD = "fields";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH,
                    "Field names on the record value to extract as the record key.");
    private List<String> jsonFields;


    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        jsonFields = config.getList(ConfigName.FIELD);
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
        final Map<String, Object> key = new HashMap<>(jsonFields.size());
        for (String field : jsonFields) {
            key.put(field.replace('.', '_'), extractField(ctx, field));
        }

        return newRecord(record, key);
    }

    private Object extractField(ReadContext ctx, String field) {
        return Optional.of(ctx.read("$." + field))
                .orElseThrow(() -> new NoSuchElementException("Element not found: " + field));
    }

    protected R newRecord(R record, Object updatedKey) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedKey, record.valueSchema(), record.value(), record.timestamp());
    }
}

