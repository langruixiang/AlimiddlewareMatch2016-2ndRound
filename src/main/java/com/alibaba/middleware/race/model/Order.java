package com.alibaba.middleware.race.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class Order {

    Long id;

    Map<String, KeyValue> keyValues = new HashMap<String, KeyValue>();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Map<String, KeyValue> getKeyValues() {
        return keyValues;
    }

    public void setKeyValues(Map<String, KeyValue> keyValues) {
        this.keyValues = keyValues;
    }

    @Override public String toString() {
        return "Order{" +
               "id=" + id +
               ", keyValues=" + keyValues +
               '}';
    }
}
