package com.alibaba.middleware.race.orderSystemImpl;

import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.HashFile;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.orderSystemInterface.OrderSystem;

import java.io.IOException;
import java.util.*;

/**
 * Created by jiangchao on 2016/7/11.
 */
public class OrderSystemImpl implements OrderSystem {

    @Override
    public void construct(Collection<String> orderFiles, Collection<String> buyerFiles,
                                    Collection<String> goodFiles, Collection<String> storeFolders)
            throws IOException, InterruptedException {

        HashFile.generateOrderIdHashFile(orderFiles, buyerFiles, goodFiles, null, FileConstant.FILE_NUMS);

        PageCache.cacheFile(13);

    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {

        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
        Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> keyValueMap = new HashMap<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue>();
        int hashIndex = (int) (orderId % 25);
        try {
            PageCache.cacheFile(hashIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Order order = PageCache.orderMap.get(orderId);
        if (order == null) {
            System.out.println("要查询的订单数据不再缓存中");
        }
        System.out.println(order);
        keyValueMap.putAll(order.getKeyValues());
        String goodid = order.getKeyValues().get("goodid").getValue();
        String buyerid = order.getKeyValues().get("buyerid").getValue();
        Good good = null;
        Buyer buyer = null;
        if (goodid != null) {
            good = PageCache.goodMap.get(goodid);
            keyValueMap.putAll(good.getKeyValues());
        }
        if (buyerid != null) {
            buyer = PageCache.buyerMap.get(buyerid);
            keyValueMap.putAll(buyer.getKeyValues());
        }
        for (String key : keys) {
            if (keyValueMap.containsKey(key)) {
                result.getKeyValues().put(key, keyValueMap.get(key));
            }
        }
        result.setOrderid(orderId);
        return result;
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        List<com.alibaba.middleware.race.orderSystemImpl.Result> results = new ArrayList<com.alibaba.middleware.race.orderSystemImpl.Result>();
        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            try {
                PageCache.cacheFile(i);
                Iterator iter = PageCache.orderMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    Long key = (Long) entry.getKey();
                    Order val = (Order) entry.getValue();
                    if (val.getKeyValues().get("buyerid") != null && val.getKeyValues().get("buyerid").getValue().equals(buyerid)
                            && val.getKeyValues().get("createtime") != null && Long.valueOf(val.getKeyValues().get("createtime").getValue()) >= startTime && Long.valueOf(val.getKeyValues().get("createtime").getValue()) <= endTime) {
                        System.out.println("======================" + val);
                        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
                        result.setOrderid(key);
                        result.setKeyValues(val.getKeyValues());
                        results.add(result);
                    }
                    //System.out.println(entry.getKey().toString() + ": " + entry.getValue());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //对所求结果按照交易时间从大到小排序
        Collections.sort(results, new Comparator<com.alibaba.middleware.race.orderSystemImpl.Result>() {

            @Override public int compare(com.alibaba.middleware.race.orderSystemImpl.Result o1,
                                         com.alibaba.middleware.race.orderSystemImpl.Result o2) {
                return o2.get("createtime").getValue().compareTo(o1.get("createtime").getValue());
            }
        });
        return results.iterator();
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        List<com.alibaba.middleware.race.orderSystemImpl.Result> results = new ArrayList<com.alibaba.middleware.race.orderSystemImpl.Result>();
        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            try {
                PageCache.cacheFile(i);
                Iterator iter = PageCache.orderMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    Long id = (Long) entry.getKey();
                    Order val = (Order) entry.getValue();
                    if (val.getKeyValues().get("goodid") != null && val.getKeyValues().get("goodid").getValue().equals(goodid)) {
                        System.out.println("===========: " + val);
                        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
                        for (String key : keys) {
                            if (val.getKeyValues().containsKey(key)) {
                                result.getKeyValues().put(key, val.getKeyValues().get(key));
                            }
                        }
                        result.setOrderid(id);
                        results.add(result);
                    }
                    //System.out.println(entry.getKey().toString() + ": " + entry.getValue());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //对所求结果按照交易订单从小到大排序
        Collections.sort(results, new Comparator<com.alibaba.middleware.race.orderSystemImpl.Result>() {

            @Override public int compare(com.alibaba.middleware.race.orderSystemImpl.Result o1,
                                         com.alibaba.middleware.race.orderSystemImpl.Result o2) {
                return o1.get("amount").getValue().compareTo(o2.get("amount").getValue());
            }
        });

        return results.iterator();
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        com.alibaba.middleware.race.orderSystemImpl.KeyValue keyValue = new com.alibaba.middleware.race.orderSystemImpl.KeyValue();
        double value = 0;
        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            try {
                PageCache.cacheFile(i);
                Iterator iter = PageCache.orderMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    Order val = (Order) entry.getValue();
                    if (val.getKeyValues().get("goodid") != null && val.getKeyValues().get("goodid").getValue().equals(goodid)) {
                        System.out.println("=============" + val);
                        value += Double.valueOf(val.getKeyValues().get(key).getValue());
                    }
                    //System.out.println(entry.getKey().toString() + ": " + entry.getValue());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        keyValue.setKey(key);
        keyValue.setValue(String.valueOf(value));
        return keyValue;
    }

}
