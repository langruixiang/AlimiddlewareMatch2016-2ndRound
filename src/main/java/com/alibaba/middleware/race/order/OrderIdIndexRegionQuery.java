/**
 * OrderIdIndexRegionQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import com.alibaba.middleware.stringindex.StringIndex;
import com.alibaba.middleware.stringindex.StringIndexHash;
import com.alibaba.middleware.stringindex.StringIndexRegion;
import com.alibaba.middleware.stringindex.StringIndexRegionQuery;

/**
 * @author wangweiwei
 *
 */
public class OrderIdIndexRegionQuery extends StringIndexRegionQuery {
    public static final int INIT_KEY_MAP_CAPACITY = 20;// TODO
    public static final int MAX_INDEX_CACHE_SIZE = 1000;// TODO
    public static final int CACHE_NUM_PER_MISS = 100;// TODO

    public OrderIdIndexRegionQuery(String regionRootFolder, int regionId) {
        super(regionRootFolder, "orderid", regionId, INIT_KEY_MAP_CAPACITY, MAX_INDEX_CACHE_SIZE, CACHE_NUM_PER_MISS);
        this.regionRootFolder = regionRootFolder;
        maxIndexCacheSize = MAX_INDEX_CACHE_SIZE;
        cacheNumPerMiss = CACHE_NUM_PER_MISS;
    }

    public com.alibaba.middleware.race.orderSystemImpl.Result queryOrder(long orderId, Collection<String> keys) {
        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
        LinkedList<String> filteredKeys = new LinkedList<String>();
        if (keys == null) {
            for (Entry<String, Integer> entry : keyMap.entrySet()) {
                String key = entry.getKey();
                filteredKeys.add(key);
            }
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
        }
        StringIndex stringIndex = getStringIndex(String.valueOf(orderId), filteredKeys);
        if (stringIndex == null) {
            return null;
        } else {
            result.setOrderid(orderId);
            for (String key : filteredKeys) {
                KeyValue keyValue = new KeyValue();
                keyValue.setKey(key);
                keyValue.setValue(stringIndex.getValueByKey(key));
                result.getKeyValues().put(key, keyValue);
            }
        }
        return result;
    }
    
    public static int getRegionIdByOrderId(long orderId, int regionNumber) {
        return StringIndexHash.getRegionIdByHashId(String.valueOf(orderId), regionNumber);
    }
    
    public static boolean isRegionExist(String regionRootFolder, int regionId) {
        return StringIndexRegion.isRegionExist(regionRootFolder, regionId);
    }
}
