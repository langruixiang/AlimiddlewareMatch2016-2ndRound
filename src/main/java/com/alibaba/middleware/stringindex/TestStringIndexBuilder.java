/**
 * TestOrderIndexAndQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.order.OrderQuery;
import com.alibaba.middleware.race.orderSystemImpl.Result;
import com.alibaba.middleware.race.util.FileUtil;

/**
 * @author wangweiwei
 *
 */
public class TestStringIndexBuilder {
    
    public static final String REGION_ROOT_DIR = "StringIndexRegion/";
    public static final String HASH_INDEX_INDEX_ID_NAME = "orderid";// TODO
    public static final int REGION_NUMBER = 10;// TODO
    public static final int HASH_WRITER_THREAD_POOL_SIZE = 10;
    public static final int REGION_INDEX_BUILDER_THREAD_POOL_SIZE = 10;
    public static final int INIT_KEY_MAP_CAPACITY = 20;

    public static void main(String args[]) {
        try {
            System.out.println("=============start================");
//            testStringIndexHash();
//            testIndexBuilder();
//          testQueryOrder();
          System.out.println("=============end================");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * @throws InterruptedException 
     * 
     */
    private static void testStringIndexHash() throws InterruptedException {
        List<String> orderFileList = new ArrayList<String>();
        orderFileList.add("order_records.txt");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        StringIndexHash sirh = new StringIndexHash(
                orderFileList, REGION_ROOT_DIR, REGION_NUMBER,
                HASH_INDEX_INDEX_ID_NAME, INIT_KEY_MAP_CAPACITY,
                countDownLatch, HASH_WRITER_THREAD_POOL_SIZE);
        sirh.start();
        countDownLatch.await();
    }

    private static void testIndexBuilder() throws InterruptedException {
        List<String> orderFileList = new ArrayList<String>();
        orderFileList.add("order_records.txt");

        List<String> buyerFileList = new ArrayList<String>();
        buyerFileList.add("buyer_records.txt");

        List<String> goodFileList = new ArrayList<String>();
        goodFileList.add("good_records.txt");
        
        List<String> storeFolderList = new ArrayList<String>();
        storeFolderList.add(REGION_ROOT_DIR);
        storeFolderList.add(REGION_ROOT_DIR);
        storeFolderList.add(REGION_ROOT_DIR);
        
        CountDownLatch countDownLatch = new CountDownLatch(1);
        StringIndexBuilder sib = new StringIndexBuilder(
                storeFolderList.get(0), REGION_NUMBER,
                HASH_INDEX_INDEX_ID_NAME, INIT_KEY_MAP_CAPACITY,
                countDownLatch, REGION_INDEX_BUILDER_THREAD_POOL_SIZE);
        sib.start();
        countDownLatch.await();
    }
    
    private static void testQueryOrder() {
        // 测试queryOrder接口，按订单号查找某条记录
        System.out.println("测试queryOrder接口，按订单号查找某条记录: ");
//        String regionRootFolder, String indexIdName, String regionId
        OrderQuery orderQuery = new OrderQuery(REGION_ROOT_DIR, REGION_NUMBER);

        List<String> keys = new ArrayList<String>();
        keys.add("buyerid");
        keys.add("amount");
        Result result = orderQuery.queryOrder(2982270, keys);
        System.out.println(result.get("buyerid").getValue());
        System.out.println(result.get("amount").getValue());
    }
}
