/**
 * OrderQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

/**
 * @author wangweiwei
 *
 */
public class OrderQuery {
    //存储订单信息
    public static OrderRegionCache orderRegionCache;
    public static HashMap<String, Integer> keyMap;
    
    public static final int MAX_REGION_CACHE_SIZE = 10;//TODO

    static {
        orderRegionCache = new OrderRegionCache(MAX_REGION_CACHE_SIZE);
        
        keyMap = FileUtil.readSIHashMapFromFile(OrderIndexBuilder.ORDER_KEY_MAP_FILE, OrderIndexBuilder.INIT_KEY_MAP_CAPACITY);
    }

    public com.alibaba.middleware.race.orderSystemImpl.Result queryOrder(long orderId, Collection<String> keys) {
        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
        LinkedList<String> filteredKeys = new LinkedList<String>();
        if (keys == null) {
            for (Entry<String, Integer> entry : keyMap.entrySet()) {
                String key = entry.getKey();
                filteredKeys.add(key);
            }
            filteredKeys.add("orderid");
        } else if (keys.isEmpty()) {
        } else {
            //TODO remove if buyer and good index are ready
            for (String key : keys) {
                if (keyMap.containsKey(key)) {
                    filteredKeys.add(key);
                }
            }
            if (!filteredKeys.contains("buyerid")) {
                filteredKeys.add("buyerid");
            }
            if (!filteredKeys.contains("goodid")) {
                filteredKeys.add("goodid");
            }
            if (keys.contains("orderid")) {
                filteredKeys.add("orderid");
            }
        }
        OrderIdIndex orderIdIndex = getOrderIdIndex(orderId, filteredKeys);
        if (orderIdIndex == null) {
            return null;
        } else {
            result.setOrderid(orderId);
            for (String key : filteredKeys) {
                result.getKeyValues().put(key, getKeyValueByOrderIdIndexAndKey(orderIdIndex, key));
            }
        }
        return result;
    }
    
    /**
     * @param orderId
     * @param keys
     * @return
     */
    private OrderIdIndex getOrderIdIndex(long orderId, Collection<String> filteredKeys) {
        int regionIndex = (int) (orderId / OrderRegion.REGION_SIZE);
        OrderRegion orderRegion = null;
        synchronized(orderRegionCache) {
            orderRegion = orderRegionCache.get(regionIndex);
            if (orderRegion == null) {
                if (!OrderRegion.isRegionExist(regionIndex)) {
                    return null;
                } else {
                    orderRegion = OrderRegion.create(regionIndex);
                    orderRegionCache.put(regionIndex, orderRegion);
                }
            }
        }
        
        return orderRegion.getOrderIdIndex(orderId, filteredKeys);
    }

    /**
     * @param orderIdIndex
     * @param key
     * @return
     */
    private KeyValue getKeyValueByOrderIdIndexAndKey(OrderIdIndex orderIdIndex, String key) {
        
        KeyValue keyValue = new KeyValue();
        keyValue.setKey(key);
        keyValue.setValue(orderIdIndex.getValueByKey(key));
        return keyValue;
    }
}
