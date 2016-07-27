package com.alibaba.middleware.race;

import java.io.File;
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
import com.alibaba.middleware.race.file.OrderHashFile;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.order.OrderIdIndexFile;
import com.alibaba.middleware.race.order.OrderIdQuery;
import com.alibaba.middleware.race.unused.FileUtil;

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
            
            for(int i = 0; i < FileConstant.FOLDER_ORDER_NUMS; i++){
            	FileUtil.createDir(FileConstant.FIRST_DISK_PATH + FileConstant.ORDER_ID_FOLDERNAME + i);
            	FileUtil.createDir(FileConstant.SECOND_DISK_PATH + FileConstant.BUYER_ID_FOLDERNAME + i);
            	FileUtil.createDir(FileConstant.THIRD_DISK_PATH + FileConstant.GOOD_ID_FOLDERNAME + i);
            }
        }

        CountDownLatch goodAndBuyerCountDownLatch = new CountDownLatch(5);

        //CountDownLatch orderIndexBuilderCountDownLatch = new CountDownLatch(1);
        System.out.println("begin to build index:");
        
        //将商品文件hash成多个小文件
        long goodTime = System.currentTimeMillis();
        GoodHashFile goodHashFileThread = new GoodHashFile(goodFiles, storeFolders, FileConstant.FILE_GOOD_NUMS, goodAndBuyerCountDownLatch);
        goodHashFileThread.start();
        //goodCountDownLatch.await();
        System.out.println("good file hash end, time:" + (System.currentTimeMillis() - goodTime));

        //将买家文件hash成多个小文件
        long buyerTime = System.currentTimeMillis();
        BuyerHashFile buyerHashFile = new BuyerHashFile(buyerFiles, storeFolders, FileConstant.FILE_BUYER_NUMS, goodAndBuyerCountDownLatch);
        buyerHashFile.start();
        //buyerCountDownLatch.await();
        System.out.println("buyer file hash end, time:" + (System.currentTimeMillis() - buyerTime));

        //按买家ID hash成多个小文件
        long buyerIdHashTime = System.currentTimeMillis();
        OrderHashFile buyerIdHashThread = new OrderHashFile(orderFiles, storeFolders, FileConstant.FILE_ORDER_NUMS, "buyerid", goodAndBuyerCountDownLatch);
        buyerIdHashThread.start();
        //buyerIdCountDownLatch.await();
        System.out.println("buyerid hash order end, time:" + (System.currentTimeMillis() - buyerIdHashTime));

        //按商品ID hash成多个小文件
        long goodIdHashTime = System.currentTimeMillis();
        OrderHashFile goodIdHashThread = new OrderHashFile(orderFiles, storeFolders, FileConstant.FILE_ORDER_NUMS, "goodid", goodAndBuyerCountDownLatch);
        goodIdHashThread.start();
        //goodIdCountDownLatch.await();
        System.out.println("goodid hash order end, time:" + (System.currentTimeMillis() - goodIdHashTime));

        //按订单ID hash成多个小文件
        long orderIdHashTime = System.currentTimeMillis();
        OrderHashFile orderIdHashThread = new OrderHashFile(orderFiles, storeFolders, FileConstant.FILE_ORDER_NUMS, "orderid", goodAndBuyerCountDownLatch);
        orderIdHashThread.start();
        //orderIdCountDownLatch.await();
        System.out.println("orderid hash order end, time:" + (System.currentTimeMillis() - orderIdHashTime));

        goodAndBuyerCountDownLatch.await();
        System.out.println("all the hash is end, time :" + (System.currentTimeMillis() - goodTime));

        ExecutorService buyerIndexThreadPool = Executors.newFixedThreadPool(6);
        ExecutorService goodIndexThreadPool = Executors.newFixedThreadPool(6);
        
        CountDownLatch buildIndexLatch = new CountDownLatch(FileConstant.FILE_GOOD_NUMS + FileConstant.FILE_BUYER_NUMS);
        
        //buyer文件生成索引放入内存
        for (int i = 0; i < FileConstant.FILE_BUYER_NUMS; i++) {
            BuyerIndexFile buyerIndexFile = new BuyerIndexFile(goodAndBuyerCountDownLatch, buildIndexLatch, i);
            buyerIndexThreadPool.execute(buyerIndexFile);
        }

        //good文件生成索引放入内存
        for (int i = 0; i < FileConstant.FILE_GOOD_NUMS; i++) {
            GoodIndexFile goodIndexFile = new GoodIndexFile(goodAndBuyerCountDownLatch, buildIndexLatch, i);
            goodIndexThreadPool.execute(goodIndexFile);
        }
        
        buildIndexLatch.await();

        long endTime = System.currentTimeMillis();
        //System.out.println("second end is :" + endTime + " second parse need time :" + (endTime - secondParseTime));
        System.out.println("all build index work end!!!!!!! the total time is :" + (endTime - beginTime));

    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        return OrderIdQuery.findOrder(orderId, keys);
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
    	
        return OldBuyerIdQuery.findOrdersByBuyer(startTime, endTime, buyerid);
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        return OldGoodIdQuery.findOrdersByGood(salerid, goodid, keys);
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        return OldGoodIdQuery.sumValuesByGood(goodid, key);
    }
}
