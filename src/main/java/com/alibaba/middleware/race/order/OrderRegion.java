/**
 * OrderRegion.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.LinkedList;

import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
/**
 * @author wangweiwei
 *
 */
public class OrderRegion {

    public static final int REGION_SIZE = 10000;
    
    public static final String ORDER_REGION_PREFIX = OrderIndexBuilder.ORDER_ID_INDEX_DIR + "/" + "order_region_";
    public static final String ORDER_ID_INDEX_PREFIX = ORDER_REGION_PREFIX + "orderid_index_";
    public static final int INIT_SINGLE_REGION_FILE_NUM = 10;
    
    //UTF_8("UTF-8"), GB2312("GB2312"), GBK("GBK");
    public static final String ENCODING = "UTF-8";
    public static final int BYTES_OF_ORDER_ID_INDEX_FILE_LINE = 1000;//TODO

    private int regionIndex;

    public static OrderIdIndexCache orderIdIndexCache;
    public static final int MAX_ORDER_ID_INDEX_CACHE_SIZE = 1000;//TODO
    public static final int CACHE_NUM_PER_MISS = 100;//TODO
    
    static {
        orderIdIndexCache = new OrderIdIndexCache(MAX_ORDER_ID_INDEX_CACHE_SIZE);
    }

    private OrderRegion(){};

    public static OrderRegion create(int inputRegionIndex) {
        OrderRegion ret = new OrderRegion();
        ret.regionIndex = inputRegionIndex;
        return ret;
    }
    
    public static String getOrderIdIndexFilePathByRegionIndex(long regionIdx) {
        return OrderRegion.ORDER_ID_INDEX_PREFIX.concat(String.valueOf(regionIdx));
    }
    
    public static String getFilePathByRegionIndexAndKey(long regionIdx, String key) {
            return OrderRegion.ORDER_REGION_PREFIX
                    .concat(String.valueOf(regionIdx)).concat("_").concat(key);
    }

    /**
     * @param orderId
     * @param keys
     * @return
     */
    public OrderIdIndex getOrderIdIndex(long orderId, Collection<String> keys) {
        OrderIdIndex orderIdIndex = null;
        synchronized (orderIdIndexCache) {
            orderIdIndex = orderIdIndexCache.get(orderId);
            if (orderIdIndex == null) {
                orderIdIndex = loadOrderIdIndexFromFile(orderId);
                orderIdIndex = loadKeyValuesOfOrderIdIndex(orderIdIndex, keys);
                orderIdIndexCache.put(orderId, orderIdIndex);
                cacheOrderIdIndexCacheNearBy(orderId, keys);
                return orderIdIndex;
            } else if (orderIdIndex == OrderIdIndex.NULL){
                return null;
            } else {
                return loadKeyValuesOfOrderIdIndex(orderIdIndex, keys);
            }
        }
        
    }

    /**
     * @param orderId
     */
    private OrderIdIndex loadOrderIdIndexFromFile(long orderId) {
        long regionIndex = orderId / OrderRegion.REGION_SIZE;
        long lineIndex = orderId % OrderRegion.REGION_SIZE;
        String regionOrderIdIndexFilePath = OrderRegion.getOrderIdIndexFilePathByRegionIndex(regionIndex);
        String line = FileUtil.getFixedBytesLine(regionOrderIdIndexFilePath, OrderRegion.ENCODING, OrderRegion.BYTES_OF_ORDER_ID_INDEX_FILE_LINE, lineIndex, true);
        OrderIdIndex orderIdIndex = OrderIdIndex.parseFromLine(line);
        return orderIdIndex;
    }

    /**
     * @param orderId
     * @param keys
     */
    private void cacheOrderIdIndexCacheNearBy(long orderId,
            Collection<String> keys) {
        long startOrderId = orderId + 1;
        long endOrderId = orderId + CACHE_NUM_PER_MISS;
        long maxRegionOrderId = (regionIndex + 1) * REGION_SIZE - 1;
        endOrderId = endOrderId > maxRegionOrderId ? maxRegionOrderId : endOrderId;
        for (long i = startOrderId; i <= endOrderId; ++i ) {
            OrderIdIndex orderIdIndex = loadOrderIdIndexFromFile(orderId);
            orderIdIndex = loadKeyValuesOfOrderIdIndex(orderIdIndex, keys);
            orderIdIndexCache.put(orderId, orderIdIndex);
        }
    }

    /**
     * @param orderIdIndex
     * @param keys
     */
    private OrderIdIndex loadKeyValuesOfOrderIdIndex(OrderIdIndex orderIdIndex, Collection<String> keys) {
        if (orderIdIndex == OrderIdIndex.NULL) {
            return orderIdIndex;
        }
        LinkedList<String> needLoadKeys = orderIdIndex.getNeedLoadKeys(keys);
        for (String key : needLoadKeys) {
            if (key.equals("orderid")) {
                orderIdIndex.addKeyValue("orderid", String.valueOf(orderIdIndex.getId()));
            } else {
                String regionKeyFilePath = getFilePathByRegionIndexAndKey(regionIndex, key);
                RandomAccessFile rf = null;
                try {
                    rf = new RandomAccessFile(regionKeyFilePath, "r");
                    Integer keyIndex = OrderQuery.keyMap.get(key);
                    orderIdIndex.loadKeyValue(keyIndex, key, rf);
                    rf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return orderIdIndex;
    }
    
    /**
     * @param orderId
     * @param key
     * @return
     */
    public static String getKeyValueDirectlyFromFile(long orderId, String key) {
        String value = null;
        long regionIndex = orderId / OrderRegion.REGION_SIZE;
        long lineIndex = orderId % OrderRegion.REGION_SIZE;
        String regionOrderIdIndexFilePath = getOrderIdIndexFilePathByRegionIndex(regionIndex);
        String line = FileUtil.getFixedBytesLine(regionOrderIdIndexFilePath, OrderRegion.ENCODING, BYTES_OF_ORDER_ID_INDEX_FILE_LINE, lineIndex, true);
        String[] splitOfLine = line.split(OrderIndexBuilder.INDEX_SPLITOR);
        String[] keysPos = splitOfLine[1].split("\\|");
        Integer keyIndex = OrderQuery.keyMap.get(key);
        
        if (keyIndex.intValue() < keysPos.length) {
            long keyPos = Integer.parseInt(keysPos[keyIndex.intValue()]);
            if (keyPos >= 0) {
                String regionKeyFilePath = getFilePathByRegionIndexAndKey(regionIndex, key);
                RandomAccessFile rf = null;
                try {
                    rf = new RandomAccessFile(regionKeyFilePath, "r");
                    value = FileUtil.getLineWithRandomAccessFile(rf, OrderRegion.ENCODING, keyPos);
                    rf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return value;
    }

    /**
     * @return
     */
    public static boolean isRegionExist(long regionIdx) {
        return FileUtil.isFileExist(getOrderIdIndexFilePathByRegionIndex(regionIdx));
    }

}
