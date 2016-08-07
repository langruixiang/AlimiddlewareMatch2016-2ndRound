package com.alibaba.middleware.race.unused;


import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.buyer.BuyerQuery;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.good.GoodQuery;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.KeyValue;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.model.Result;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class BuyerIdQuery {
    public static List<Order> findByBuyerId(String buyerId, long starttime, long endtime, int index) {
        if (buyerId == null || buyerId.isEmpty()) return null;
        String beginKey = buyerId + "_" + starttime;
        String endKey = buyerId + "_" + endtime;
        List<Order> orders = new ArrayList<Order>();
        try {

            File rankFile = new File(Config.SECOND_DISK_PATH + FileConstant.FILE_RANK_BY_BUYERID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "r");

            File indexFile = new File(Config.SECOND_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_BUYERID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "r");
            String str = null;

            //1.查找二·级索引
            long position = TwoIndexCache.findBuyerIdOneIndexPosition(buyerId, starttime, endtime, index);

            //2.查找一级索引
            long oneIndexStartTime = System.currentTimeMillis();
            int count = 0;
            indexRaf.seek(position);
            String oneIndex = null;
            String oneIndexTmp = null;
            String onePlusIndex = null;
            while ((oneIndexTmp = indexRaf.readLine()) != null) {
                count++;
                String[] keyValue = oneIndexTmp.split(":");
                if (endKey.compareTo(keyValue[0]) <= 0) {
                    continue;
                } else if (beginKey.compareTo(keyValue[0]) > 0) {
                    onePlusIndex = oneIndexTmp;
                    break;
                }
                if (oneIndex == null) {
                    oneIndex = oneIndexTmp;
                }
            }
            if (oneIndex == null) return null;

            //3.按行读取内容
            long handleStartTime = System.currentTimeMillis();
            System.out.println(oneIndex);
            String[] keyValue = oneIndex.split(":");
            String pos = keyValue[1];
            int length = 0;
            if (onePlusIndex != null) {
                String[] kv = onePlusIndex.split(":");
                System.out.println(kv[1] + ":" + pos);
                length = (int) (Long.valueOf(kv[1]) - Long.valueOf(pos) -1);
            } else {
                System.out.println(hashRaf.length() + ":" + pos);
                length = (int) (hashRaf.length() - Long.valueOf(pos));
            }
            hashRaf.seek(Long.valueOf(pos));
            byte[] bytes = new byte[length];
            hashRaf.read(bytes, 0, length);
            String orderStrs = new String(bytes);
            String[] constents = orderStrs.split("\n");
            for (String orderContent : constents) {
                //4.将字符串转成order对象集合
                Order order = new Order();
                String[] keyValues = orderContent.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] strs = keyValues[i].split(":");
                    KeyValue kv = new KeyValue();
                    kv.setKey(strs[0]);
                    kv.setValue(strs[1]);
                    order.getKeyValues().put(strs[0], kv);
                }
                if (order.getKeyValues().get("orderid").getValue() != null && NumberUtils.isNumber(order.getKeyValues().get("orderid").getValue())){
                    order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));
                }
                orders.add(order);
            }
            hashRaf.close();
            indexRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orders;
    }

    public static Iterator<Result> unusedFindOrdersByBuyer(long startTime, long endTime, String buyerid) {
        long starttime = System.currentTimeMillis();
        List<com.alibaba.middleware.race.model.Result> results = new ArrayList<com.alibaba.middleware.race.model.Result>();
        int hashIndex = (int) (Math.abs(buyerid.hashCode()) % Config.ORDER_ONE_INDEX_FILE_NUMBER);

        int buyerHashIndex = (int) (Math.abs(buyerid.hashCode()) % Config.FILE_BUYER_NUMS);
        Buyer buyer = BuyerQuery.findBuyerById(buyerid);
        if (buyer == null) return results.iterator();

        //获取goodid的所有订单信息
        List<Order> orders = BuyerIdQuery.findByBuyerId(buyerid, startTime, endTime, hashIndex);
        if (orders == null || orders.size() == 0) return results.iterator();

        for (Order order : orders) {
            com.alibaba.middleware.race.model.Result result = new com.alibaba.middleware.race.model.Result();
            //加入对应买家的所有属性kv
            if (buyer != null && buyer.getKeyValues() != null) {
                result.getKeyValues().putAll(buyer.getKeyValues());
            }
            //加入对应商品的所有属性kv
            int goodIdHashIndex = (int) (Math.abs(order.getKeyValues().get("goodid").getValue().hashCode()) % Config.FILE_GOOD_NUMS);
            Good good = GoodQuery.findGoodById(order.getKeyValues().get("goodid").getValue());

            if (good != null && good.getKeyValues() != null) {
                result.getKeyValues().putAll(good.getKeyValues());
            }
            //加入订单信息的所有属性kv
            result.getKeyValues().putAll(order.getKeyValues());
            result.setOrderid(order.getId());
            results.add(result);
        }
        return results.iterator();
    }
}
