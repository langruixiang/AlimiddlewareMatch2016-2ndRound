package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.middleware.race.buyer.*;
import com.alibaba.middleware.race.good.*;

import org.apache.commons.lang3.math.NumberUtils;

import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.BuyerHashFile;
import com.alibaba.middleware.race.file.GoodHashFile;
import com.alibaba.middleware.race.file.NewOrderHashFile;
import com.alibaba.middleware.race.file.OrderHashFile;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.order.NewOrderIdQuery;
import com.alibaba.middleware.race.order.OrderIdIndexFile;
import com.alibaba.middleware.race.order.OrderIdQuery;

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
        ExecutorService buyerIndexThreadPool = Executors.newFixedThreadPool(10);
        ExecutorService goodIndexThreadPool = Executors.newFixedThreadPool(10);

        CountDownLatch goodIdCountDownLatch = new CountDownLatch(1);
        CountDownLatch buyerIdCountDownLatch = new CountDownLatch(1);
        CountDownLatch orderIdCountDownLatch = new CountDownLatch(1);
        CountDownLatch buyerCountDownLatch = new CountDownLatch(1);
        CountDownLatch goodCountDownLatch = new CountDownLatch(1);
        //CountDownLatch goodAndBuyerCountDownLatch = new CountDownLatch(2);
        CountDownLatch buildIndexLatch = new CountDownLatch(3 * FileConstant.FILE_NUMS);
        CountDownLatch goodAndBuyerBuildIndexLatch = new CountDownLatch(2 * FileConstant.FILE_NUMS);
        //CountDownLatch orderIndexBuilderCountDownLatch = new CountDownLatch(1);

//        //按买家ID hash成多个小文件
//        OrderHashFile buyerIdHashThread = new OrderHashFile(orderFiles, storeFolders, FileConstant.FILE_NUMS, "buyerid", buyerIdCountDownLatch);
//        buyerIdHashThread.start();
//
//        //按商品ID hash成多个小文件
//        OrderHashFile goodIdHashThread = new OrderHashFile(orderFiles, storeFolders, FileConstant.FILE_NUMS, "goodid", goodIdCountDownLatch);
//        goodIdHashThread.start();

        //将商品文件hash成多个小文件
        GoodHashFile goodHashFileThread = new GoodHashFile(goodFiles, storeFolders, FileConstant.FILE_NUMS, goodCountDownLatch);
        goodHashFileThread.start();


        //将买家文件hash成多个小文件
        BuyerHashFile buyerHashFile = new BuyerHashFile(buyerFiles, storeFolders, FileConstant.FILE_NUMS, buyerCountDownLatch);
        buyerHashFile.start();

        //buyer文件生成索引放入内存
        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            BuyerIndexFile buyerIndexFile = new BuyerIndexFile(buyerCountDownLatch, goodAndBuyerBuildIndexLatch, i);
            buyerIndexThreadPool.execute(buyerIndexFile);
        }

        //good文件生成索引放入内存
        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            GoodIndexFile goodIndexFile = new GoodIndexFile(goodCountDownLatch, goodAndBuyerBuildIndexLatch, i);
            goodIndexThreadPool.execute(goodIndexFile);
        }

        goodAndBuyerBuildIndexLatch.await();
        System.out.println("goodAndBuyerBuildIndexLatch time is :" + (System.currentTimeMillis() - beginTime));

        GoodQuery.initGoodHashFiles();
        BuyerQuery.initBuyerHashFiles();
        //按订单ID hash成多个小文件并同时合并相应的good和buyer的信息
        NewOrderHashFile orderIdHashThread = new NewOrderHashFile(orderFiles, storeFolders, FileConstant.FILE_NUMS, "orderid", goodAndBuyerBuildIndexLatch, orderIdCountDownLatch);
        orderIdHashThread.start();
        orderIdCountDownLatch.await();
        System.out.println("orderIdCountDownLatch time is :" + (System.currentTimeMillis() - beginTime));

        NewOrderHashFile buyerIdHashThread = new NewOrderHashFile(orderFiles, storeFolders, FileConstant.FILE_NUMS, "buyerid", goodAndBuyerBuildIndexLatch, buyerIdCountDownLatch);
        buyerIdHashThread.start();
        buyerIdCountDownLatch.await();
        System.out.println("buyerIdCountDownLatch time is :" + (System.currentTimeMillis() - beginTime));

        NewOrderHashFile goodIdHashThread = new NewOrderHashFile(orderFiles, storeFolders, FileConstant.FILE_NUMS, "goodid", goodAndBuyerBuildIndexLatch, goodIdCountDownLatch);
        goodIdHashThread.start();
        goodIdCountDownLatch.await();
        GoodQuery.closeGoodHashFiles();
        BuyerQuery.closeBuyerHashFiles();
        System.out.println("goodIdCountDownLatch time is :" + (System.currentTimeMillis() - beginTime));
//        System.exit(1);

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
        //goodAndBuyerCountDownLatch.await();
        //long midTime = System.currentTimeMillis();
        //System.out.println("midTime end is :" + midTime + " one parse need time :" + (midTime - beginTime));
        //orderIndexBuilderCountDownLatch.await();
        long endTime = System.currentTimeMillis();
        //System.out.println("second end is :" + endTime + " second parse need time :" + (endTime - secondParseTime));
        System.out.println("all build index work end!!!!!!! the total time is :" + (endTime - beginTime));

    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        return NewOrderIdQuery.findOrder(orderId, keys);
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        return NewBuyerIdQuery.findOrdersByBuyer(startTime, endTime, buyerid);
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        return NewGoodIdQuery.findOrdersByGood(salerid, goodid, keys);
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        return NewGoodIdQuery.sumValuesByGood(goodid, key);
    }
}
