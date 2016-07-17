/**
 * OrderQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.util.Collection;

import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import com.alibaba.middleware.race.orderSystemInterface.OrderSystem.Result;

/**
 * @author wangweiwei
 *
 */
public class OrderQuery {
    //存储订单信息
    public static OrderRegionCache orderRegionCache;
    
    public static final int MAX_CACHE_SIZE = 10;//TODO

    static {
        orderRegionCache = new OrderRegionCache(MAX_CACHE_SIZE);
    }

    public Result queryOrder(long orderId, Collection<String> keys) {
        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();

        for (String key : keys) {
            result.getKeyValues().put(key, getKeyValueByOrderIdAndKey(orderId, key));
        }
        result.setOrderid(orderId);
        return result;
    }
    
    /**
     * @param orderId
     * @param key
     * @return
     */
    private KeyValue getKeyValueByOrderIdAndKey(long orderId, String key) {
        int regionIndex = (int) (orderId / OrderIndexBuilder.REGION_SIZE);
        OrderRegion orderRegion = orderRegionCache.get(regionIndex);
        if (orderRegion == null) {
            orderRegion = OrderRegion.create(regionIndex);
            orderRegionCache.put(regionIndex, orderRegion);
        }

        String value = orderRegion.getAttribute(orderId, key);
        KeyValue ret = new KeyValue();
        ret.setKey(key);
        ret.setValue(value);
        return ret;
    }
}
