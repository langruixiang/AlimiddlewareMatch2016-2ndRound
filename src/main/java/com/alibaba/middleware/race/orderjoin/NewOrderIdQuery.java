package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.buyer.BuyerQuery;
import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
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
public class NewOrderIdQuery {
    public static Order findByOrderId(long orderId, int index) {
        Order order = new Order();
        try {

            File hashFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_INDEX_BY_ORDERID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");

            File indexFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_ORDERID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = TwoIndexCache.findOrderIdOneIndexPosition(orderId, index);

            //2.查找一级索引
            indexRaf.seek(position);
            String oneIndex = null;
            int count = 0;
            while ((oneIndex = indexRaf.readLine()) != null) {
                String[] keyValue = oneIndex.split(":");
                if (orderId == Long.valueOf(keyValue[0])) {
                    break;
                }
                count++;
                if (count >= FileConstant.orderIdIndexRegionSizeMap.get(index)) {
                    return null;
                }
            }

            //3.按行读取内容
            String[] keyValue = oneIndex.split(":");

            long pos = Long.valueOf(keyValue[1]);
            hashRaf.seek(Long.valueOf(pos));
            String orderContent = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");

            //4.将字符串转成order对象集合
            String[] keyValues = orderContent.split("\t");
            for (int i = 0; i < keyValues.length; i++) {
                String[] strs = keyValues[i].split(":");
                KeyValue kv = new KeyValue();
                kv.setKey(strs[0]);
                kv.setValue(strs[1]);
                order.getKeyValues().put(strs[0], kv);
            }
            order.setId(orderId);
            hashRaf.close();
            indexRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return order;
    }

    public static OrderSystem.Result findOrder(long orderId, Collection<String> keys) {
        System.out.println("===queryOrder=====orderid:" + orderId + "======keys:" + keys);
        long starttime = System.currentTimeMillis();
        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
        int hashIndex = (int) (orderId % FileConstant.FILE_ORDER_NUMS);
        long findStartTime = System.currentTimeMillis();
        Order order = NewOrderIdQuery.findByOrderId(orderId, hashIndex);
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
        if (keys == null) {
            result.getKeyValues().putAll(order.getKeyValues());
        } else {
            for (String key : orderSearchKeys) {
                if (order.getKeyValues().containsKey(key)) {
                    result.getKeyValues().put(key, order.getKeyValues().get(key));
                }
            }
            for (String key : buyerSearchKeys) {
                if (order.getKeyValues().containsKey(key)) {
                    result.getKeyValues().put(key, order.getKeyValues().get(key));
                }
            }
            for (String key : goodSearchKeys) {
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
