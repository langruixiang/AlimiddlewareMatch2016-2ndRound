/**
 * OrderIdIndexRegionQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map.Entry;

import com.alibaba.middleware.race.model.KeyValue;
import com.alibaba.middleware.race.model.Result;
import com.alibaba.middleware.unused.stringindex.StringIndex;
import com.alibaba.middleware.unused.stringindex.StringIndexHash;
import com.alibaba.middleware.unused.stringindex.StringIndexRegion;
import com.alibaba.middleware.unused.stringindex.StringIndexRegionQuery;

/**
 * @author wangweiwei
 *
 */
public class NewOrderIdIndexRegionQuery extends StringIndexRegionQuery {

    public NewOrderIdIndexRegionQuery(String regionRootFolder, int regionId) {
        super(regionRootFolder, "orderid", regionId,
                NewOrderIdIndexConfig.INIT_KEY_MAP_CAPACITY,
                NewOrderIdIndexConfig.MAX_INDEX_CACHE_SIZE,
                NewOrderIdIndexConfig.CACHE_NUM_PER_MISS);
        if (!regionRootFolder.endsWith("/")) {
            regionRootFolder = regionRootFolder.concat("/");
        }
        this.regionRootFolder = regionRootFolder;
        maxIndexCacheSize = NewOrderIdIndexConfig.MAX_INDEX_CACHE_SIZE;
        cacheNumPerMiss = NewOrderIdIndexConfig.CACHE_NUM_PER_MISS;
    }

    public Result queryOrder(long orderId, Collection<String> keys) {
        Result result = new Result();
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
            result.setOrderId(orderId);
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
