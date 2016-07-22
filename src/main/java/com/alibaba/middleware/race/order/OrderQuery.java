/**
 * OrderQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

/**
 * @author wangweiwei
 *
 */
public class OrderQuery {
    public static final int DEFAULT_MAX_INDEX_CACHE_SIZE = 1000;
    public static final int DEFAULT_CACHE_NUM_PER_MISS = 100;
    
    private HashMap<Integer, OrderIdIndexRegionQuery> regionQueryMap;
    
    public static final int REGION_NUM = 10;
    
    private int regionNumber;

    private String regionRootFolder;

    public OrderQuery(String regionRootFolder, int regionNumber) {
        this.regionRootFolder = regionRootFolder;
        this.regionNumber = regionNumber;
        regionQueryMap = new HashMap<Integer, OrderIdIndexRegionQuery>(regionNumber);
        for (int i = 0; i < regionNumber; ++i) {
            regionQueryMap.put(i, new OrderIdIndexRegionQuery(regionRootFolder, i));
        }
    }

    public com.alibaba.middleware.race.orderSystemImpl.Result queryOrder(long orderId, Collection<String> keys) {
        int regionId = OrderIdIndexRegionQuery.getRegionIdByOrderId(orderId, regionNumber);
        if (!OrderIdIndexRegionQuery.isRegionExist(regionRootFolder, regionId)) {
            return null;
        }
        OrderIdIndexRegionQuery regionQuery = regionQueryMap.get(regionId);
        return regionQuery.queryOrder(orderId, keys);
    }
}
