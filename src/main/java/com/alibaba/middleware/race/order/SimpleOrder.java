package com.alibaba.middleware.race.order;

import java.util.HashMap;
import java.util.Map;

public class SimpleOrder {

    long id;
    
    long regionIndex;

    Map<String, String> keyValueMap = new HashMap<String, String>();

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getRegionIndex() {
        return regionIndex;
    }

    public void setRegionIndex(long regionIndex) {
        this.regionIndex = regionIndex;
    }
    
    public void addKeyValue(String key, String value) {
        keyValueMap.put(key, value);
    }

    public Map<String, String> getKeyValueMap() {
        return keyValueMap;
    }
}
