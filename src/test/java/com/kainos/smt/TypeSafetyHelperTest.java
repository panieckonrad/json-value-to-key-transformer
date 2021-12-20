package com.kainos.smt;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TypeSafetyHelperTest {

    @Test
    void throwsDataExceptionWhenValueNotString() {
        Assertions.assertThrows(DataException.class, () -> {
            TypeSafetyHelper.requireString(5, "test");
        });
    }
}