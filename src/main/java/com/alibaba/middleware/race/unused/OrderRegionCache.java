package com.alibaba.middleware.race.unused;

import java.util.LinkedHashMap;
import java.util.Map;

public class OrderRegionCache extends LinkedHashMap<Integer, OrderRegion> {

    private static final long serialVersionUID = -5009472213760319410L;
    private int maxCacheSize;

    public OrderRegionCache(int maxCacheSize) {
        super(maxCacheSize, 1);
        this.maxCacheSize = maxCacheSize;
    }

    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() >= maxCacheSize;
    }
}
