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
    private HashMap<String, OrderIdIndexRegionQuery> regionQueryMap;
    
    public static final int REGION_NUM = 10;
    
    private int regionNumber;

    private String regionRootFolder;

    public OrderQuery(String regionRootFolder, int regionNumber) {
        this.regionRootFolder = regionRootFolder;
        this.regionNumber = regionNumber;
        regionQueryMap = new HashMap<String, OrderIdIndexRegionQuery>(regionNumber);
        for (int i = 0; i < regionNumber; ++i) {
            regionQueryMap.put(String.valueOf(i), new OrderIdIndexRegionQuery(regionRootFolder, String.valueOf(i)));
        }
    }

    public com.alibaba.middleware.race.orderSystemImpl.Result queryOrder(long orderId, Collection<String> keys) {
        String regionId = OrderIdIndexRegionQuery.getRegionIdByOrderId(orderId, regionNumber);
        if (!OrderIdIndexRegionQuery.isRegionExist(regionRootFolder, regionId)) {
            return null;
        }
        OrderIdIndexRegionQuery regionQuery = regionQueryMap.get(regionId);
        return regionQuery.queryOrder(orderId, keys);
    }
}
