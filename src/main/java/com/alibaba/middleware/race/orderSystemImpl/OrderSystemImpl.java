package com.alibaba.middleware.race.orderSystemImpl;

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
import com.alibaba.middleware.race.orderSystemInterface.OrderSystem;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
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
        ExecutorService buyerIdIndexThreadPool = Executors.newFixedThreadPool(10);
        ExecutorService goodIdIndexThreadPool = Executors.newFixedThreadPool(10);

        CountDownLatch goodIdCountDownLatch = new CountDownLatch(1);
        CountDownLatch buyerIdCountDownLatch = new CountDownLatch(1);
        CountDownLatch buildIndexLatch = new CountDownLatch(2 * FileConstant.FILE_NUMS);

        //按买家ID hash成多个小文件
        OrderHashFile buyerIdHashThread = new OrderHashFile(orderFiles, buyerFiles, goodFiles, null, FileConstant.FILE_NUMS, "buyerid", buyerIdCountDownLatch);
        buyerIdHashThread.start();

        //按商品ID hash成多个小文件
        OrderHashFile goodIdHashThread = new OrderHashFile(orderFiles, buyerFiles, goodFiles, null, FileConstant.FILE_NUMS, "goodid", goodIdCountDownLatch);
        goodIdHashThread.start();


        //将商品文件hash成多个小文件
        GoodHashFile goodHashFileThread = new GoodHashFile(goodFiles, null, FileConstant.FILE_NUMS);
        goodHashFileThread.start();


        //将买家文件hash成多个小文件
        BuyerHashFile buyerHashFile = new BuyerHashFile(buyerFiles, null, FileConstant.FILE_NUMS);
        buyerHashFile.start();

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

        buildIndexLatch.await();
        System.out.println("all work end!!!!!!!");

    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {

        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
        return result;
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        List<com.alibaba.middleware.race.orderSystemImpl.Result> results = new ArrayList<com.alibaba.middleware.race.orderSystemImpl.Result>();
        int hashIndex = (int) (Math.abs(buyerid.hashCode()) % FileConstant.FILE_NUMS);

        //获取goodid的所有订单信息
        List<Order> orders = BuyerIdQuery.findByBuyerId(buyerid, startTime, endTime, hashIndex);
        if (orders == null || orders.size() == 0) return null;
        //Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> keyValueMap = new HashMap<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue>();
        for (Order order : orders) {
            com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
            //加入对应买家的所有属性kv
            if (PageCache.buyerMap.get(hashIndex) == null) {
                PageCache.cacheBuyerFile(hashIndex);
            }
            Buyer buyer = PageCache.buyerMap.get(hashIndex).get(buyerid);
            if (buyer != null && buyer.getKeyValues() != null) {
                result.getKeyValues().putAll(buyer.getKeyValues());
            }
            //加入对应商品的所有属性kv
            int goodIdHashIndex = (int) (Math.abs(order.getKeyValues().get("goodid").getValue().hashCode()) % FileConstant.FILE_NUMS);
            if (PageCache.goodMap.get(goodIdHashIndex) == null) {
                PageCache.cacheGoodFile(goodIdHashIndex);
            }
            Good good = PageCache.goodMap.get(goodIdHashIndex).get(order.getKeyValues().get("goodid").getValue());
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

        for (com.alibaba.middleware.race.orderSystemImpl.Result result : results) {
            System.out.println(result);
        }

        return results.iterator();
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        //flag为1表示查询所有字段
        int flag = 0;
        if (keys == null) {
            flag = 1;
        } else if (goodid == null || keys.size() == 0) {
            return null;
        }
        List<com.alibaba.middleware.race.orderSystemImpl.Result> results = new ArrayList<com.alibaba.middleware.race.orderSystemImpl.Result>();
        int hashIndex = (int) (Math.abs(goodid.hashCode()) % FileConstant.FILE_NUMS);
        //获取goodid的所有订单信息
        List<Order> orders = GoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0) return null;

        for (Order order : orders) {
            Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> keyValueMap = new HashMap<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue>();
            //加入对应买家的所有属性kv
            int buyeridHashIndex = (int) (Math.abs(order.getKeyValues().get("buyerid").getValue().hashCode()) % FileConstant.FILE_NUMS);
            if (PageCache.buyerMap.get(buyeridHashIndex) == null) {
                PageCache.cacheBuyerFile(buyeridHashIndex);
            }
            Buyer buyer = PageCache.buyerMap.get(buyeridHashIndex).get(order.getKeyValues().get("buyerid").getValue());
            if (buyer != null && buyer.getKeyValues() != null) {
                keyValueMap.putAll(buyer.getKeyValues());
            }
            //加入对应商品的所有属性kv
            if (PageCache.goodMap.get(hashIndex) == null) {
                PageCache.cacheGoodFile(hashIndex);
            }
            Good good = PageCache.goodMap.get(hashIndex).get(goodid);
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
                double diff = 0;
                try {
                    diff = (o1.get("amount").valueAsDouble() - o2.get("amount").valueAsDouble());
                } catch (TypeException e) {
                    e.printStackTrace();
                }
                if (diff > 0) {
                    return 1;
                }
                return -1;
            }
        });

        for (com.alibaba.middleware.race.orderSystemImpl.Result result : results) {
            System.out.println(result);
        }

        return results.iterator();
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        if (goodid == null || key == null) return null;
        com.alibaba.middleware.race.orderSystemImpl.KeyValue keyValue = new com.alibaba.middleware.race.orderSystemImpl.KeyValue();
        int hashIndex = (int) (Math.abs(goodid.hashCode()) % FileConstant.FILE_NUMS);
        List<Order> orders = GoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0) return null;
        double value = 0;
        int count = 0;
        for (Order order : orders) {
            //加入订单信息的所有属性kv
            if (order.getKeyValues().containsKey(key)) {
                String str = order.getKeyValues().get(key).getValue();
                if (NumberUtils.isNumber(str)) {
                    value += Double.valueOf(str);
                    count++;
                    continue;
                }
                return null;
            }

            //加入对应买家的所有属性kv
            int buyeridHashIndex = (int) (Math.abs(order.getKeyValues().get("buyerid").getValue().hashCode()) % FileConstant.FILE_NUMS);
            if (PageCache.buyerMap.get(buyeridHashIndex) == null) {
                PageCache.cacheBuyerFile(buyeridHashIndex);
            }
            Buyer buyer = PageCache.buyerMap.get(buyeridHashIndex).get(order.getKeyValues().get("buyerid").getValue());
            if (buyer.getKeyValues().containsKey(key)) {
                String str = buyer.getKeyValues().get(key).getValue();
                if (NumberUtils.isNumber(str)) {
                    value += Double.valueOf(str);
                    count++;
                    continue;
                }
                return null;
            }
            //加入对应商品的所有属性kv
            if (PageCache.goodMap.get(hashIndex) == null) {
                PageCache.cacheGoodFile(hashIndex);
            }
            Good good = PageCache.goodMap.get(hashIndex).get(goodid);
            if (good.getKeyValues().containsKey(key)) {
                String str = good.getKeyValues().get(key).getValue();
                if (NumberUtils.isNumber(str)) {
                    value += Double.valueOf(str);
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
        keyValue.setValue(String.valueOf(value));
        return keyValue;
    }

}
