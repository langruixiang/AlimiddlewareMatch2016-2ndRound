package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jiangchao on 2016/7/11.
 */
public class Good {

    String id;

    Map<String, KeyValue> keyValues = new HashMap<String, KeyValue>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, KeyValue> getKeyValues() {
        return keyValues;
    }

    public void setKeyValues(Map<String, KeyValue> keyValues) {
        this.keyValues = keyValues;
    }

    @Override public String toString() {
        return "Good{" +
               "id='" + id + '\'' +
               ", keyValues=" + keyValues +
               '}';
    }
}
