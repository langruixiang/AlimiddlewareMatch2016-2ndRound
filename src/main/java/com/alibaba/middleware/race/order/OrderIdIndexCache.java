package com.alibaba.middleware.race.order;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OrderIdIndexCache extends ConcurrentHashMap<Long, Long> {
    private static final long serialVersionUID = -8423112410864183141L;
    private int maxCacheSize;

    public OrderIdIndexCache(int maxCacheSize) {
        super(maxCacheSize, 1);
        this.maxCacheSize = maxCacheSize;
    }

    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() >= maxCacheSize;
    }
    
    public void setMaxCacheSize(int size) {
        if (size > 0) {
            maxCacheSize = size;
        }
    }
}
