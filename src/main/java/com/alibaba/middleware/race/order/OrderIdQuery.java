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
public class OrderIdQuery {
    public static Order findByOrderId(long orderId, int index) {
        //System.out.println("==========:"+goodId + " index:" + index);
        Order order = new Order();
        try {
//            FileInputStream twoIndexFile = new FileInputStream(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_TWO_INDEXING_BY_ORDERID + index);
//            BufferedReader twoIndexBR = new BufferedReader(new InputStreamReader(twoIndexFile));

            File hashFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_INDEX_BY_ORDERID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");

            File indexFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_ORDERID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = TwoIndexCache.findOrderIdOneIndexPosition(orderId, index);
//            while ((str = twoIndexBR.readLine()) != null) {
//                String[] keyValue = str.split(":");
//                //System.out.println(keyValue[0]);
//                if (orderId < Long.valueOf(keyValue[0])) {
//                    //System.out.println("--------"+keyValue[0]);
//                    break;
//                } else {
//                    position = Long.valueOf(keyValue[1]);
//                }
//            }

            //System.out.println(position);

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
            //System.out.println(keyValue[1]);

            long pos = Long.valueOf(keyValue[1]);
            //System.out.println(pos);
            hashRaf.seek(Long.valueOf(pos));
            String orderContent = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
            //System.out.println(orderContent);

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
//            twoIndexBR.close();
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
        int hashIndex = (int) (orderId % FileConstant.FILE_NUMS);
        long findStartTime = System.currentTimeMillis();
        Order order = OrderIdQuery.findByOrderId(orderId, hashIndex);
        System.out.println("===queryOrder==index===orderid: " + orderId + " time :" + (System.currentTimeMillis() - findStartTime));
        List<String> orderSearchKeys = new ArrayList<String>();
        List<String> goodSearchKeys = new ArrayList<String>();
        List<String> buyerSearchKeys = new ArrayList<String>();
        if (keys != null) {
            for (String key : keys) {
                if (KeyCache.orderKeyCache.contains(key)) {
                    System.out.println("===queryOrder=====orderid:" + orderId + "======key in order");
                    orderSearchKeys.add(key);
                } else if (KeyCache.goodKeyCache.contains(key)) {
                    System.out.println("===queryOrder=====orderid:" + orderId + "======key in good");
                    goodSearchKeys.add(key);
                } else if (KeyCache.buyerKeyCache.contains(key)) {
                    System.out.println("===queryOrder=====orderid:" + orderId + "======key in buyer");
                    buyerSearchKeys.add(key);
                }
            }
        }
        if (order == null) {
            System.out.println("orderid: " + orderId + "is null");
            return null;
        }
        if (keys != null && keys.isEmpty()) {
            result.setOrderid(orderId);
            System.out.println(orderId + ": keys is empty" );
            return result;
        }
        {
            if (keys == null || buyerSearchKeys.size() > 0) {
                String buyerId = order.getKeyValues().get("buyerid").getValue();
                int buyerHashIndex = (int) (Math.abs(buyerId.hashCode()) % FileConstant.FILE_NUMS);
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
                int goodIdHashIndex = (int) (Math.abs(goodId.hashCode()) % FileConstant.FILE_NUMS);
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
