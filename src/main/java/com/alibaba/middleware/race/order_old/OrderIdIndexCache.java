//package com.alibaba.middleware.race.order_old;
//
//import java.util.LinkedHashMap;
//import java.util.Map;
//
//public class OrderIdIndexCache extends LinkedHashMap<Long, OrderIdIndex> {
//
//    private static final long serialVersionUID = -5009472213760319410L;
//    private int maxCacheSize;
//
//    public OrderIdIndexCache(int maxCacheSize) {
//        super(maxCacheSize, 1);
//        this.maxCacheSize = maxCacheSize;
//    }
//
//    protected boolean removeEldestEntry(Map.Entry eldest) {
//        return size() >= maxCacheSize;
//    }
//}
