package com.alibaba.middleware.race;

import com.alibaba.middleware.race.buyer.BuyerIdIndexFile;
import com.alibaba.middleware.race.buyer.BuyerIdQuery;
import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.BuyerHashFile;
import com.alibaba.middleware.race.file.GoodHashFile;
import com.alibaba.middleware.race.file.OrderHashFile;
import com.alibaba.middleware.race.good.GoodIdIndexFile;
import com.alibaba.middleware.race.good.GoodIdQuery;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.order.OrderIdIndexFile;
import com.alibaba.middleware.race.order.OrderIdQuery;
import com.alibaba.middleware.race.orderSystemImpl.Result;
import com.alibaba.middleware.race.order_old.OrderIndexBuilder;
import com.alibaba.middleware.race.order_old.OrderQuery;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jiangchao on 2016/7/11.
 */
public class OrderSystemImpl implements OrderSystem {

    //实现无参构造函数
    public OrderSystemImpl() {

    }

    @Override
    public void construct(final Collection<String> orderFiles, final Collection<String> buyerFiles,
                          final Collection<String> goodFiles, Collection<String> storeFolders)
            throws IOException, InterruptedException {
        long beginTime = System.currentTimeMillis();
        if (storeFolders != null && storeFolders.size() >= 3) {
            FileConstant.FIRST_DISK_PATH = FileConstant.FIRST_DISK_PATH + storeFolders.toArray()[0];
            FileConstant.SECOND_DISK_PATH = FileConstant.SECOND_DISK_PATH + storeFolders.toArray()[1];
            FileConstant.THIRD_DISK_PATH = FileConstant.THIRD_DISK_PATH + storeFolders.toArray()[2];
        }

        ExecutorService orderIdIndexThreadPool = Executors.newFixedThreadPool(10);
        ExecutorService buyerIdIndexThreadPool = Executors.newFixedThreadPool(10);
        ExecutorService goodIdIndexThreadPool = Executors.newFixedThreadPool(10);

        CountDownLatch goodIdCountDownLatch = new CountDownLatch(1);
        CountDownLatch buyerIdCountDownLatch = new CountDownLatch(1);
        CountDownLatch orderIdCountDownLatch = new CountDownLatch(1);
        CountDownLatch goodAndBuyerCountDownLatch = new CountDownLatch(2);
        CountDownLatch buildIndexLatch = new CountDownLatch(3 * FileConstant.FILE_NUMS);
        //CountDownLatch orderIndexBuilderCountDownLatch = new CountDownLatch(1);

        //按买家ID hash成多个小文件
        OrderHashFile buyerIdHashThread = new OrderHashFile(orderFiles, storeFolders, FileConstant.FILE_NUMS, "buyerid", buyerIdCountDownLatch);
        buyerIdHashThread.start();

        //按商品ID hash成多个小文件
        OrderHashFile goodIdHashThread = new OrderHashFile(orderFiles, storeFolders, FileConstant.FILE_NUMS, "goodid", goodIdCountDownLatch);
        goodIdHashThread.start();

        //按订单ID hash成多个小文件
        OrderHashFile orderIdHashThread = new OrderHashFile(orderFiles, storeFolders, FileConstant.FILE_NUMS, "orderid", orderIdCountDownLatch);
        orderIdHashThread.start();


        //将商品文件hash成多个小文件
        GoodHashFile goodHashFileThread = new GoodHashFile(goodFiles, storeFolders, FileConstant.FILE_NUMS, goodAndBuyerCountDownLatch);
        goodHashFileThread.start();


        //将买家文件hash成多个小文件
        BuyerHashFile buyerHashFile = new BuyerHashFile(buyerFiles, storeFolders, FileConstant.FILE_NUMS, goodAndBuyerCountDownLatch);
        buyerHashFile.start();

        //根据orderid生成一级二级索引
        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            OrderIdIndexFile orderIdIndexFile = new OrderIdIndexFile(orderIdCountDownLatch, buildIndexLatch, i);
            orderIdIndexThreadPool.execute(orderIdIndexFile);
        }

        //根据buyerid生成一级二级索引
        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            BuyerIdIndexFile buyerIdIndexFile = new BuyerIdIndexFile(buyerIdCountDownLatch, buildIndexLatch, i);
            buyerIdIndexThreadPool.execute(buyerIdIndexFile);
        }

        //根据goodid生成一级二级索引
        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            GoodIdIndexFile goodIdIndexFile = new GoodIdIndexFile(goodIdCountDownLatch, buildIndexLatch, i);
            goodIdIndexThreadPool.execute(goodIdIndexFile);
        }

        //long secondParseTime = System.currentTimeMillis();
        //根据orderid建立索引以及文件
        //OrderIndexBuilder orderIndexBuilder = new OrderIndexBuilder(orderFiles, storeFolders, orderIndexBuilderCountDownLatch);
        //orderIndexBuilder.start();

        buildIndexLatch.await();
        goodAndBuyerCountDownLatch.await();
        //long midTime = System.currentTimeMillis();
        //System.out.println("midTime end is :" + midTime + " one parse need time :" + (midTime - beginTime));
        //orderIndexBuilderCountDownLatch.await();
        long endTime = System.currentTimeMillis();
        //System.out.println("second end is :" + endTime + " second parse need time :" + (endTime - secondParseTime));
        System.out.println("all build index work end!!!!!!! the total time is :" + (endTime - beginTime));

    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        //System.out.println("===queryOrder=====orderid:" + orderId + "======keys:" + keys);
        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
        int hashIndex = (int) (orderId % FileConstant.FILE_NUMS);
        Order order = OrderIdQuery.findByOrderId(orderId, hashIndex);
        if (order == null) {
            return null;
        }
        if (keys != null && keys.isEmpty()) {
            result.setOrderid(orderId);
            //System.out.println(orderId + ": " + result);
            return result;
        }
        {
            String buyerId = order.getKeyValues().get("buyerid").getValue();
            int buyerHashIndex = (int) (Math.abs(buyerId.hashCode()) % FileConstant.FILE_NUMS);
            //加入对应买家的所有属性kv
            Buyer buyer = null;
            synchronized (PageCache.buyerMap) {
                if (PageCache.buyerMap.get(buyerHashIndex) == null) {
                    PageCache.cacheBuyerFile(buyerHashIndex);
                }
                buyer = PageCache.buyerMap.get(buyerHashIndex).get(buyerId);
            }
            if (buyer != null && buyer.getKeyValues() != null) {
                if (keys ==  null) {
                    result.getKeyValues().putAll(buyer.getKeyValues());
                } else {
                    Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> buyerKeyValues = buyer.getKeyValues();
                    for (String key : keys) {
                        if (buyerKeyValues.containsKey(key)) {
                            result.getKeyValues().put(key, buyerKeyValues.get(key));
                        }
                    }
                }
            }
        }

        {
            String goodId = order.getKeyValues().get("goodid").getValue();
            //加入对应商品的所有属性kv
            int goodIdHashIndex = (int) (Math.abs(goodId.hashCode()) % FileConstant.FILE_NUMS);
            Good good = null;
            synchronized (PageCache.goodMap) {
                if (PageCache.goodMap.get(goodIdHashIndex) == null) {
                    PageCache.cacheGoodFile(goodIdHashIndex);
                }
                good = PageCache.goodMap.get(goodIdHashIndex).get(goodId);
            }
            if (good != null && good.getKeyValues() != null) {
                if (keys ==  null) {
                    result.getKeyValues().putAll(good.getKeyValues());
                } else {
                    Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> goodKeyValues = good.getKeyValues();
                    for (String key : keys) {
                        if (goodKeyValues.containsKey(key)) {
                            result.getKeyValues().put(key, goodKeyValues.get(key));
                        }
                    }
                }
            }
        }
        if (keys == null) {
            result.getKeyValues().putAll(order.getKeyValues());
        } else {
            for (String key : keys) {
                if (order.getKeyValues().containsKey(key)) {
                    result.getKeyValues().put(key, order.getKeyValues().get(key));
                }
            }
        }
        result.setOrderid(orderId);
//        if (result != null) {
//            System.out.println(orderId + ": " + result);
//        }
        return result;
    }

//    @Override
//    public Result queryOrder(long orderId, Collection<String> keys) {
//        System.out.println("===queryOrder=====orderid:" + orderId + "======keys:" + keys);
//        OrderQuery orderQuery = new OrderQuery();
//        com.alibaba.middleware.race.orderSystemImpl.Result result = orderQuery.queryOrder(orderId, keys);
//        if (result == null) {
//            return null;
//        }
//        if (keys != null && keys.isEmpty()) {
//            return result;
//        }
//        {
//            String buyerId = result.get("buyerid").getValue();
//            if (keys != null && !keys.contains("buyerid")) {
//                result.remove("buyerid");
//            }
//            int hashIndex = (int) (Math.abs(buyerId.hashCode()) % FileConstant.FILE_NUMS);
//            //加入对应买家的所有属性kv
//            Buyer buyer = null;
//            synchronized (PageCache.buyerMap) {
//                if (PageCache.buyerMap.get(hashIndex) == null) {
//                    PageCache.cacheBuyerFile(hashIndex);
//                }
//                buyer = PageCache.buyerMap.get(hashIndex).get(buyerId);
//            }
//            if (buyer != null && buyer.getKeyValues() != null) {
//                if (keys ==  null) {
//                    result.getKeyValues().putAll(buyer.getKeyValues());
//                } else {
//                    Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> buyerKeyValues = buyer.getKeyValues();
//                    for (String key : keys) {
//                        if (buyerKeyValues.containsKey(key)) {
//                            result.getKeyValues().put(key, buyerKeyValues.get(key));
//                        }
//                    }
//                }
//            }
//        }
//
//        {
//            String goodId = result.get("goodid").getValue();
//            if (keys != null && !keys.contains("goodid")) {
//                result.remove("goodid");
//            }
//            //加入对应商品的所有属性kv
//            int goodIdHashIndex = (int) (Math.abs(goodId.hashCode()) % FileConstant.FILE_NUMS);
//            Good good = null;
//            synchronized (PageCache.goodMap) {
//                if (PageCache.goodMap.get(goodIdHashIndex) == null) {
//                    PageCache.cacheGoodFile(goodIdHashIndex);
//                }
//                good = PageCache.goodMap.get(goodIdHashIndex).get(goodId);
//            }
//            if (good != null && good.getKeyValues() != null) {
//                if (keys ==  null) {
//                    result.getKeyValues().putAll(good.getKeyValues());
//                } else {
//                    Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> goodKeyValues = good.getKeyValues();
//                    for (String key : keys) {
//                        if (goodKeyValues.containsKey(key)) {
//                            result.getKeyValues().put(key, goodKeyValues.get(key));
//                        }
//                    }
//                }
//            }
//        }
//        if (result != null) {
//            System.out.println(orderId + ": " + result.toString());
//        }
//        return result;
//    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        //System.out.println("===queryOrdersByBuyer=====buyerid:" + buyerid + "======starttime:" + startTime + "=========endtime:" + endTime);
        List<com.alibaba.middleware.race.orderSystemImpl.Result> results = new ArrayList<com.alibaba.middleware.race.orderSystemImpl.Result>();
        int hashIndex = (int) (Math.abs(buyerid.hashCode()) % FileConstant.FILE_NUMS);

        //获取goodid的所有订单信息
        List<Order> orders = BuyerIdQuery.findByBuyerId(buyerid, startTime, endTime, hashIndex);
        if (orders == null || orders.size() == 0) return results.iterator();
        //Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> keyValueMap = new HashMap<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue>();
        for (Order order : orders) {
            //System.out.println("queryOrdersByBuyer buyerid:"+ buyerid +" : " + order_old.toString());
            com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
            //加入对应买家的所有属性kv
            Buyer buyer = null;
            synchronized (PageCache.buyerMap) {
                if (PageCache.buyerMap.get(hashIndex) == null) {
                    PageCache.cacheBuyerFile(hashIndex);
                }
                buyer = PageCache.buyerMap.get(hashIndex).get(buyerid);
            }
            if (buyer != null && buyer.getKeyValues() != null) {
                result.getKeyValues().putAll(buyer.getKeyValues());
            }
            //加入对应商品的所有属性kv
            Good good = null;
            synchronized (PageCache.goodMap) {
                int goodIdHashIndex = (int) (Math.abs(order.getKeyValues().get("goodid").getValue().hashCode()) % FileConstant.FILE_NUMS);
                if (PageCache.goodMap.get(goodIdHashIndex) == null) {
                    PageCache.cacheGoodFile(goodIdHashIndex);
                }
                good = PageCache.goodMap.get(goodIdHashIndex).get(order.getKeyValues().get("goodid").getValue());
            }
            if (good != null && good.getKeyValues() != null) {
                result.getKeyValues().putAll(good.getKeyValues());
            }
            //加入订单信息的所有属性kv
            result.getKeyValues().putAll(order.getKeyValues());
            result.setOrderid(order.getId());
            results.add(result);
        }

        //对所求结果按照交易时间从大到小排序
//        Collections.sort(results, new Comparator<com.alibaba.middleware.race.orderSystemImpl.Result>() {
//
//            @Override public int compare(com.alibaba.middleware.race.orderSystemImpl.Result o1,
//                                         com.alibaba.middleware.race.orderSystemImpl.Result o2) {
//                return o2.get("createtime").getValue().compareTo(o1.get("createtime").getValue());
//            }
//        });

//        for (com.alibaba.middleware.race.orderSystemImpl.Result result : results) {
//            System.out.println(buyerid + result);
//        }

        return results.iterator();
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        //System.out.println("===queryOrdersBySaler=====goodid:" + goodid + "======keys:" + keys);
        List<com.alibaba.middleware.race.orderSystemImpl.Result> results = new ArrayList<com.alibaba.middleware.race.orderSystemImpl.Result>();
        //flag为1表示查询所有字段
        int flag = 0;
        if (keys == null) {
            flag = 1;
        } else if (goodid == null) {
            return results.iterator();
        }
        int hashIndex = (int) (Math.abs(goodid.hashCode()) % FileConstant.FILE_NUMS);
        //获取goodid的所有订单信息
        List<Order> orders = GoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0) return results.iterator();
        if (keys != null && keys.size() == 0) {
            for (Order order : orders) {
                com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
                result.setOrderid(order.getId());
                results.add(result);
            }
            //对所求结果按照交易订单从小到大排序
            Collections.sort(results, new Comparator<com.alibaba.middleware.race.orderSystemImpl.Result>() {

                @Override
                public int compare(com.alibaba.middleware.race.orderSystemImpl.Result o1,
                                   com.alibaba.middleware.race.orderSystemImpl.Result o2) {
                    long diff = 0;
                    diff = (o1.getOrderid() - o2.getOrderid());
                    if (diff > 0) {
                        return 1;
                    }
                    return -1;
                }
            });
            return results.iterator();
        }
        for (Order order : orders) {
            //System.out.println("queryOrdersBySaler goodid:"+ goodid +" : " + order_old.toString());
            Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> keyValueMap = new HashMap<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue>();
            //加入对应买家的所有属性kv
            int buyeridHashIndex = (int) (Math.abs(order.getKeyValues().get("buyerid").getValue().hashCode()) % FileConstant.FILE_NUMS);
            Buyer buyer = null;
            synchronized (PageCache.buyerMap) {
                if (PageCache.buyerMap.get(buyeridHashIndex) == null) {
                    PageCache.cacheBuyerFile(buyeridHashIndex);
                }
                buyer = PageCache.buyerMap.get(buyeridHashIndex).get(order.getKeyValues().get("buyerid").getValue());
            }
            if (buyer != null && buyer.getKeyValues() != null) {
                keyValueMap.putAll(buyer.getKeyValues());
            }
            //加入对应商品的所有属性kv
            Good good = null;
            synchronized (PageCache.goodMap) {
                if (PageCache.goodMap.get(hashIndex) == null) {
                    PageCache.cacheGoodFile(hashIndex);
                }
                good = PageCache.goodMap.get(hashIndex).get(goodid);
            }
            if (good != null && good.getKeyValues() != null) {
                keyValueMap.putAll(good.getKeyValues());
            }
            //加入订单信息的所有属性kv
            keyValueMap.putAll(order.getKeyValues());

            com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
            if (flag == 1) {
                result.setKeyValues(keyValueMap);
            } else {
                for (String key : keys) {
                    if (keyValueMap.containsKey(key)) {
                        result.getKeyValues().put(key, keyValueMap.get(key));
                    }
                }
            }
            result.setOrderid(order.getId());
            results.add(result);
        }
        //对所求结果按照交易订单从小到大排序
        Collections.sort(results, new Comparator<com.alibaba.middleware.race.orderSystemImpl.Result>() {

            @Override
            public int compare(com.alibaba.middleware.race.orderSystemImpl.Result o1,
                                         com.alibaba.middleware.race.orderSystemImpl.Result o2) {
                long diff = 0;
                diff = (o1.getOrderid() - o2.getOrderid());
                if (diff > 0) {
                    return 1;
                }
                return -1;
            }
        });

//        for (com.alibaba.middleware.race.orderSystemImpl.Result result : results) {
//            System.out.println(goodid + ":" + result);
//        }

        return results.iterator();
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        //System.out.println("===sumOrdersByGood=====goodid:" + goodid + "======key:" + key);
        if (goodid == null || key == null) return null;
        com.alibaba.middleware.race.orderSystemImpl.KeyValue keyValue = new com.alibaba.middleware.race.orderSystemImpl.KeyValue();
        int hashIndex = (int) (Math.abs(goodid.hashCode()) % FileConstant.FILE_NUMS);
        List<Order> orders = GoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0) return null;
        double value = 0;
        long longValue = 0;
        int count = 0;
        //flag=0表示Long类型，1表示Double类型
        int flag = 0;
        for (Order order : orders) {
            //System.out.println("sum goodid:"+ goodid +" : " + order_old.toString());
            //加入订单信息的所有属性kv
            if (order.getKeyValues().containsKey(key)) {
                String str = order.getKeyValues().get(key).getValue();
                if (flag == 0 && str.contains(".")) {
                    flag = 1;
                }
                if (NumberUtils.isNumber(str)) {
                    if (flag == 0) {
                        longValue += Long.valueOf(str);
                        value += Double.valueOf(str);
                    } else {
                        value += Double.valueOf(str);
                    }
                    count++;
                    continue;
                }
                return null;
            }

            //加入对应买家的所有属性kv
            int buyeridHashIndex = (int) (Math.abs(order.getKeyValues().get("buyerid").getValue().hashCode()) % FileConstant.FILE_NUMS);
            Buyer buyer = null;
            synchronized (PageCache.buyerMap) {
                if (PageCache.buyerMap.get(buyeridHashIndex) == null) {
                    PageCache.cacheBuyerFile(buyeridHashIndex);
                }
                buyer = PageCache.buyerMap.get(buyeridHashIndex).get(order.getKeyValues().get("buyerid").getValue());
            }
            if (buyer.getKeyValues().containsKey(key)) {
                String str = buyer.getKeyValues().get(key).getValue();
                if (flag == 0 && str.contains(".")) {
                    flag = 1;
                }
                if (NumberUtils.isNumber(str)) {
                    if (flag == 0) {
                        longValue += Long.valueOf(str);
                        value += Double.valueOf(str);
                    } else {
                        value += Double.valueOf(str);
                    }
                    count++;
                    continue;
                }
                return null;
            }
            //加入对应商品的所有属性kv
            Good good = null;
            synchronized (PageCache.goodMap) {
                if (PageCache.goodMap.get(hashIndex) == null) {
                    PageCache.cacheGoodFile(hashIndex);
                }
                good = PageCache.goodMap.get(hashIndex).get(goodid);
            }
            if (good.getKeyValues().containsKey(key)) {
                String str = good.getKeyValues().get(key).getValue();
                if (flag == 0 && str.contains(".")) {
                    flag = 1;
                }
                if (NumberUtils.isNumber(str)) {
                    if (flag == 0) {
                        longValue += Long.valueOf(str);
                        value += Double.valueOf(str);
                    } else {
                        value += Double.valueOf(str);
                    }
                    count++;
                    continue;
                }
                return null;
            }
        }
        if (count == 0) {
            return null;
        }
        keyValue.setKey(key);
        if (flag == 0) {
            keyValue.setValue(String.valueOf(longValue));
            //System.out.println("sum goodid:"+ goodid +" : " + longValue);
        } else {
            keyValue.setValue(String.valueOf(value));
            //System.out.println("sum goodid:"+ goodid +" : " + value);
        }
        return keyValue;
    }

}
