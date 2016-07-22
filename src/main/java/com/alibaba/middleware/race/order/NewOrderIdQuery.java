/**
 * OrderIdQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.util.Collection;
import java.util.HashMap;

import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.Result;

/**
 * @author wangweiwei
 *
 */
public class NewOrderIdQuery {

    private static HashMap<Integer, NewOrderIdIndexRegionQuery> regionQueryMap;

    public static void initRegionQueryMap() {
        regionQueryMap = new HashMap<Integer, NewOrderIdIndexRegionQuery>(NewOrderIdIndexConfig.REGION_NUMBER);
        for (int i = 0; i < NewOrderIdIndexConfig.REGION_NUMBER; ++i) {
            regionQueryMap.put(i, new NewOrderIdIndexRegionQuery(NewOrderIdIndexConfig.REGION_ROOT_FOLDER, i));
        }
    }

    public static Order queryOrder(long orderId, Collection<String> keys) {
        if (regionQueryMap == null) {
            initRegionQueryMap();
        }
        int regionId = NewOrderIdIndexRegionQuery.getRegionIdByOrderId(orderId, NewOrderIdIndexConfig.REGION_NUMBER);
        if (!NewOrderIdIndexRegionQuery.isRegionExist(NewOrderIdIndexConfig.REGION_ROOT_FOLDER, regionId)) {
            return null;
        }
        NewOrderIdIndexRegionQuery regionQuery = regionQueryMap.get(regionId);
        Result result = regionQuery.queryOrder(orderId, keys);
        if (result == null) {
            return null;
        }
        Order order = new Order();
        order.setId(orderId);
        order.getKeyValues().putAll(result.getKeyValues());
        return order;
    }
}
