package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.buyer.BuyerQuery;
import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.OrderIndex;
import com.alibaba.middleware.race.good.GoodQuery;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class OrderIdQuery {
    public static Order findByOrderId(long orderId) {
        
        OrderIndex orderIndex = OrderIndex.getOrderIndexbyOrderID(orderId);
        Map<Long, Order> page;
        
        if(!PageCache.orderPageMap.containsKey(orderIndex)){
        	PageCache.cacheOrderByOrderID(orderId);
        }
        
        page = PageCache.orderPageMap.get(orderIndex);
        
        return page.get(orderId);
    }

    public static OrderSystem.Result findOrder(long orderId, Collection<String> keys) {
        System.out.println("===queryOrder=====orderid:" + orderId + "======keys:" + keys);
        long starttime = System.currentTimeMillis();
        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
        long findStartTime = System.currentTimeMillis();
        Order order = OrderIdQuery.findByOrderId(orderId);
        System.out.println("===queryOrder==index===orderid: " + orderId + " time :" + (System.currentTimeMillis() - findStartTime));
        List<String> orderSearchKeys = new ArrayList<String>();
        List<String> goodSearchKeys = new ArrayList<String>();
        List<String> buyerSearchKeys = new ArrayList<String>();
        if (keys != null) {
            for (String key : keys) {
                if (KeyCache.orderKeyCache.contains(key)) {
                    orderSearchKeys.add(key);
                } else if (KeyCache.goodKeyCache.contains(key)) {
                    goodSearchKeys.add(key);
                } else if (KeyCache.buyerKeyCache.contains(key)) {
                    buyerSearchKeys.add(key);
                }
            }
        }
        if (order == null) {
            return null;
        }
        if (keys != null && keys.isEmpty()) {
            result.setOrderid(orderId);
            return result;
        }
        {
            if (keys == null || buyerSearchKeys.size() > 0) {
                String buyerId = order.getKeyValues().get("buyerid").getValue();
                int buyerHashIndex = (int) (Math.abs(buyerId.hashCode()) % FileConstant.FILE_BUYER_NUMS);
                //加入对应买家的所有属性kv
                Buyer buyer = BuyerQuery.findBuyerById(buyerId, buyerHashIndex);

                if (buyer != null && buyer.getKeyValues() != null) {
                    if (keys ==  null) {
                        result.getKeyValues().putAll(buyer.getKeyValues());
                    } else {
                        Map<String, KeyValue> buyerKeyValues = buyer.getKeyValues();
                        for (String key : buyerSearchKeys) {
                            if (buyerKeyValues.containsKey(key)) {
                                result.getKeyValues().put(key, buyerKeyValues.get(key));
                            }
                        }
                    }
                }
            }
        }

        {
            if (keys == null || goodSearchKeys.size() > 0) {
                String goodId = order.getKeyValues().get("goodid").getValue();
                //加入对应商品的所有属性kv
                int goodIdHashIndex = (int) (Math.abs(goodId.hashCode()) % FileConstant.FILE_GOOD_NUMS);
                Good good = GoodQuery.findGoodById(goodId, goodIdHashIndex);

                if (good != null && good.getKeyValues() != null) {
                    if (keys ==  null) {
                        result.getKeyValues().putAll(good.getKeyValues());
                    } else {
                        Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> goodKeyValues = good.getKeyValues();
                        for (String key : goodSearchKeys) {
                            if (goodKeyValues.containsKey(key)) {
                                result.getKeyValues().put(key, goodKeyValues.get(key));
                            }
                        }
                    }
                }
            }

        }
        if (keys == null) {
            result.getKeyValues().putAll(order.getKeyValues());
        } else {
            for (String key : orderSearchKeys) {
                if (order.getKeyValues().containsKey(key)) {
                    result.getKeyValues().put(key, order.getKeyValues().get(key));
                }
            }
        }
        result.setOrderid(orderId);
        System.out.println("queryOrder : " + orderId + " time :" + (System.currentTimeMillis() - starttime));
        return result;
    }

    public static void main(String args[]) {

        //OrderIdIndexFile.generateGoodIdIndex();
        //findByOrderId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
