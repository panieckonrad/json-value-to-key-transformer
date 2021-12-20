package com.kainos.smt;

import net.minidev.json.JSONObject;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

class JsonValueToKeyTest {
    private final JsonValueToKey<SourceRecord> xform = new JsonValueToKey<>();
    private static String json;

    @BeforeAll
    static void initJson() {
        JSONObject obj = new JSONObject();
        obj.put("name", "foo");
        HashMap<String, Object> payload = new HashMap<>();
        payload.put("country", "EU");
        obj.put("payload", payload);
        json = obj.toJSONString();
    }

    @AfterEach
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test
    void throwsExceptionWhenInvalidJsonFields() {
        xform.configure(Collections.singletonMap("fields", "invalidField"));
        Assertions.assertThrows(RuntimeException.class, () -> {
            xform.apply(
                    new SourceRecord(null, null,
                            "", 0, null, json));
        });

    }

    @Test
    void handlesFlatJsonCorrectly() {
        xform.configure(Collections.singletonMap("fields", "name"));
        HashMap<String, Object> expected = new HashMap<>();
        expected.put("name","foo");

        SourceRecord newRecord = xform.apply(
                new SourceRecord(null, null,
                        "", 0, null, json));


        Assertions.assertNotNull(newRecord.key());
        Assertions.assertEquals(newRecord.key(), expected);
    }

    @Test
    void handlesNestedJsonCorrectly() {
        xform.configure(Collections.singletonMap("fields", "payload.country"));
        HashMap<String, Object> expected = new HashMap<>();
        expected.put("payload_country","EU");

        SourceRecord newRecord = xform.apply(
                new SourceRecord(null, null,
                        "", 0, null, json));


        Assertions.assertNotNull(newRecord.key());
        Assertions.assertEquals(newRecord.key(), expected);
    }

    @Test
    void handlesMultipleJsonFieldsCorrectly() {
        xform.configure(Collections.singletonMap("fields", "name,payload.country"));
        HashMap<String, Object> expected = new HashMap<>();
        expected.put("payload_country","EU");
        expected.put("name","foo");

        SourceRecord newRecord = xform.apply(
                new SourceRecord(null, null,
                        "", 0, null, json));


        Assertions.assertNotNull(newRecord.key());
        Assertions.assertEquals(newRecord.key(), expected);
    }
}