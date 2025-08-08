package com.milvus.options;

import java.util.HashMap;
import java.util.Map;

public class MilvusSchemaOptions {

    public static final String URI = "uri";
    public static final String User = "user";
    public static final String PassWord = "password";
    public static final String DB = "db";
    public static final String TimeOutMs = "timeout";
    public static final String UseSSL = "useSSL";

    private static final Map<String, Object> DEFAULT_VALUES;

    static {
        DEFAULT_VALUES = new HashMap<>();
        DEFAULT_VALUES.put(URI, "");
        DEFAULT_VALUES.put(User, "root");
        DEFAULT_VALUES.put(PassWord, "");
        DEFAULT_VALUES.put(DB, "default");
        DEFAULT_VALUES.put(TimeOutMs, 60000);
        DEFAULT_VALUES.put(UseSSL, false);
    }

    public static String getStringDefaultValue(String key) {
        return (String) DEFAULT_VALUES.get(key);
    }

    public static Integer getIntDefaultValue(String key) {
        return (Integer) DEFAULT_VALUES.get(key);
    }

    public static Boolean getBoolDefaultValue(String key) {
        return (Boolean) DEFAULT_VALUES.get(key);
    }

}
