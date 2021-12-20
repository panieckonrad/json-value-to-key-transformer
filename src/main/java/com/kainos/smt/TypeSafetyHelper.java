package com.kainos.smt;

import org.apache.kafka.connect.errors.DataException;

public class TypeSafetyHelper {

    public static String requireString(Object value, String purpose) {
        if (!(value instanceof String)) {
            throw new DataException("Only String objects supported for [" + purpose + "] , found: " + nullSafeClassName(value));
        } else {
            return (String) value;
        }
    }

    private static String nullSafeClassName(Object x) {
        return x == null ? "null" : x.getClass().getName();
    }
}
