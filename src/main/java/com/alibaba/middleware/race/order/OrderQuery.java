/**
 * OrderQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import com.alibaba.middleware.race.orderSystemInterface.OrderSystem.Result;

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

    public Result queryOrder(long orderId, Collection<String> keys) {
        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();

        OrderIdIndex orderIdIndex = getOrderIdIndex(orderId, keys);
        if (orderIdIndex == null) {
            return null;
        }
        result.setOrderid(orderId);
        if (keys == null) {
            for (Entry<String, Integer> entry : keyMap.entrySet()) {
                String key = entry.getKey();
                result.getKeyValues().put(key, getKeyValueByOrderIdIndexAndKey(orderIdIndex, key));
            }
        } else if (keys.isEmpty()) {
            return result;
        } else {
            for (String key : keys) {
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
    private OrderIdIndex getOrderIdIndex(long orderId, Collection<String> keys) {
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
        
        return orderRegion.getOrderIdIndex(orderId, keys);
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
