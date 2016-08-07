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
import com.alibaba.middleware.race.util.SwitchThread;

import org.apache.commons.lang3.math.NumberUtils;

import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.BuyerHashFile;
import com.alibaba.middleware.race.file.GoodHashFile;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.order.BuyerIdOneIndexBuilder;
import com.alibaba.middleware.race.order.BuyerIdTwoIndexBuilder;
import com.alibaba.middleware.race.order.GoodIdHasher;
import com.alibaba.middleware.race.order.GoodIdIndexBuilder;
import com.alibaba.middleware.race.order.OrderIdOneIndexBuilder;
import com.alibaba.middleware.race.order.OrderIdTwoIndexBuilder;
import com.alibaba.middleware.race.order.OrderIdQuery;

/**
 * Created by jiangchao on 2016/7/11.
 */
public class OrderSystemImpl implements OrderSystem {

    private static CountDownLatch buildIndexLatch = new CountDownLatch(3 * Config.ORDER_ONE_INDEX_FILE_NUMBER);
    private static CountDownLatch buyerCountDownLatch = new CountDownLatch(1);
    private static CountDownLatch goodCountDownLatch = new CountDownLatch(1);

    private static long constructStartTime;

    private static int flag = 0;
    //实现无参构造函数
    public OrderSystemImpl() {

    }

    @Override
    public void construct(final Collection<String> orderFiles, final Collection<String> buyerFiles,
                          final Collection<String> goodFiles, Collection<String> storeFolders)
            throws IOException, InterruptedException {
        constructStartTime = System.currentTimeMillis();

        // 定时器线程启动
        CountDownLatch switchCountDownLatch = new CountDownLatch(1);
        SwitchThread switchThread = new SwitchThread(switchCountDownLatch,
                buyerFiles, buildIndexLatch);
        switchThread.start();

        // 设定存储路径
        long beginTime = System.currentTimeMillis();
        if (storeFolders != null && storeFolders.size() >= 3) {
            Config.FIRST_DISK_PATH = Config.FIRST_DISK_PATH
                    + storeFolders.toArray()[0];
            Config.SECOND_DISK_PATH = Config.SECOND_DISK_PATH
                    + storeFolders.toArray()[1];
            Config.THIRD_DISK_PATH = Config.THIRD_DISK_PATH
                    + storeFolders.toArray()[2];
        }

        System.out.println("begin to build index:");

        // order files 起始编号
        int orderFilesBeginNo = goodFiles.toArray().length + buyerFiles.toArray().length;

        //按买家ID建立订单的一级索引文件(未排序)
        CountDownLatch buyerIdOneIndexBuilderLatch = new CountDownLatch(1);
        BuyerIdOneIndexBuilder buyerIdOneIndexBuilder = new BuyerIdOneIndexBuilder(orderFiles, Config.ORDER_ONE_INDEX_FILE_NUMBER, buyerIdOneIndexBuilderLatch, orderFilesBeginNo);
        buyerIdOneIndexBuilder.start();

        //按商品ID将订单hash成多个小文件(未排序)
        CountDownLatch goodIdHasherLatch = new CountDownLatch(1);
        GoodIdHasher goodIdHasher = new GoodIdHasher(orderFiles, Config.ORDER_ONE_INDEX_FILE_NUMBER, goodIdHasherLatch, orderFilesBeginNo);
        goodIdHasher.start();

        //按orderid建立order的一级索引文件(未排序)
        CountDownLatch orderIdOneIndexBuilderLatch = new CountDownLatch(1);
        OrderIdOneIndexBuilder orderIdHashThread = new OrderIdOneIndexBuilder(orderFiles, Config.ORDER_ONE_INDEX_FILE_NUMBER, orderIdOneIndexBuilderLatch, orderFilesBeginNo);
        orderIdHashThread.start();

        //根据orderid生成order的二级索引(同时生成排序的一级索引文件)
        OrderIdTwoIndexBuilder orderIdTwoIndexBuilder = new OrderIdTwoIndexBuilder(orderIdOneIndexBuilderLatch, buildIndexLatch, Config.ORDER_ID_TWO_INDEX_BUILDER_MAX_CONCURRENT_NUM);
        orderIdTwoIndexBuilder.start();

        //根据buyerid生成order的二级索引(同时生成排序的一级索引文件)
        BuyerIdTwoIndexBuilder buyerIdIndexFile = new BuyerIdTwoIndexBuilder(buyerIdOneIndexBuilderLatch, buildIndexLatch, Config.BUYER_ID_TWO_INDEX_BUILDER_MAX_CONCURRENT_NUM);
        buyerIdIndexFile.start();

        //根据goodid生成order的一级二级索引
        GoodIdIndexBuilder goodIdIndexFile = new GoodIdIndexBuilder(goodIdHasherLatch, buildIndexLatch, Config.GOOD_ID_TWO_INDEX_BUILDER_MAX_CONCURRENT_NUM);
        goodIdIndexFile.start();

        //将商品文件hash成多个小文件//TODO
        long goodTime = System.currentTimeMillis();
        GoodHashFile goodHashFileThread = new GoodHashFile(buildIndexLatch, goodFiles, storeFolders, Config.FILE_GOOD_NUMS, goodCountDownLatch, 0);
        goodHashFileThread.start();
        //goodCountDownLatch.await();


        //将买家文件hash成多个小文件
        long buyerTime = System.currentTimeMillis();
        BuyerHashFile buyerHashFile = new BuyerHashFile(buildIndexLatch, buyerFiles, storeFolders, Config.FILE_BUYER_NUMS, buyerCountDownLatch, goodFiles.toArray().length);
        buyerHashFile.start();
        //buyerCountDownLatch.await();


        //long secondParseTime = System.currentTimeMillis();
        //根据orderid建立索引以及文件
        //OrderIndexBuilder orderIndexBuilder = new OrderIndexBuilder(orderFiles, storeFolders, orderIndexBuilderCountDownLatch);
        //orderIndexBuilder.start();

        //buildIndexLatch.await();
        //goodAndBuyerCountDownLatch.await();
        //long midTime = System.currentTimeMillis();
        //System.out.println("midTime end is :" + midTime + " one parse need time :" + (midTime - beginTime));
        //orderIndexBuilderCountDownLatch.await();
        switchCountDownLatch.await();
        long endTime = System.currentTimeMillis();
        //System.out.println("second end is :" + endTime + " second parse need time :" + (endTime - secondParseTime));
        System.out.println("all build index work end!!!!!!! the total time is :" + (endTime - beginTime));

    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        try {
            buildIndexLatch.await();
            goodCountDownLatch.await();
            buyerCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (flag == 0) {
            System.out.println("all the build work is end. time : " + (System.currentTimeMillis() - constructStartTime));

            flag = 1;
        }
        return OrderIdQuery.findOrder(orderId, keys);
    }

    @Override
    public Iterator<com.alibaba.middleware.race.model.Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        try {
            buildIndexLatch.await();
            goodCountDownLatch.await();
            buyerCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (flag == 0) {
            System.out.println("all the build work is end. time : " + (System.currentTimeMillis() - constructStartTime));
            flag = 1;
        }
        return BuyerIdQuery.findOrdersByBuyer(startTime, endTime, buyerid);
    }

    @Override
    public Iterator<com.alibaba.middleware.race.model.Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        try {
            buildIndexLatch.await();
            goodCountDownLatch.await();
            buyerCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (flag == 0) {
            System.out.println("all the build work is end. time : " + (System.currentTimeMillis() - constructStartTime));
            flag = 1;
        }
        return GoodIdQuery.findOrdersByGood(salerid, goodid, keys);
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        try {
            buildIndexLatch.await();
            goodCountDownLatch.await();
            buyerCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (flag == 0) {
            System.out.println("all the build work is end. time : " + (System.currentTimeMillis() - constructStartTime));
            flag = 1;
        }
        return GoodIdQuery.sumValuesByGood(goodid, key);
    }
}
