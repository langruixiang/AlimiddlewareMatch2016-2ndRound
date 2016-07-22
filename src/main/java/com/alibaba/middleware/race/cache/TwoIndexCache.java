package com.alibaba.middleware.race.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangchao on 2016/7/21.
 */
public class TwoIndexCache {

    public static Map<Integer, Map<Long, Long>> orderIdTwoIndexCache = new ConcurrentHashMap<Integer, Map<Long, Long>>();

    public static Map<Integer, Map<String, Long>> goodIdTwoIndexCache = new ConcurrentHashMap<Integer, Map<String, Long>>();

    public static Map<Integer, Map<String, Long>> buyerIdTwoIndexCache = new ConcurrentHashMap<Integer, Map<String, Long>>();

    public static long findOrderIdOneIndexPosition (long orderId, int index) {
        long position = 0;
        Iterator iterator = orderIdTwoIndexCache.get(index).entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            Long key = (Long) entry.getKey();
            Long val = (Long)entry.getValue();
            if (orderId < key) {
                break;
            } else {
                position = val;
            }
        }
        return position;
    }

    public static long findGoodIdOneIndexPosition (String goodId, int index) {
        long position = 0;
        Iterator iterator = goodIdTwoIndexCache.get(index).entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            String key = (String) entry.getKey();
            Long val = (Long)entry.getValue();
            if (goodId.compareTo(key) < 0) {
                break;
            } else {
                position = val;
            }
        }
        return position;
    }

    public static long findBuyerIdOneIndexPosition (String buyerId, long starttime, long endtime, int index) {
        long position = 0;
        String beginKey = buyerId + "_" + starttime;
        String endKey = buyerId + "_" + endtime;
        Iterator iterator = buyerIdTwoIndexCache.get(index).entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            String key = (String) entry.getKey();
            Long val = (Long)entry.getValue();
            if (endKey.compareTo(key) > 0) {
                //System.out.println("--------"+keyValue[0]);
                break;
            } else {
                position = val;
            }
        }
        return position;
    }
}
