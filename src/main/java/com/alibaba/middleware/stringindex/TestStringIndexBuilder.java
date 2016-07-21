/**
 * TestOrderIndexAndQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.order.OrderIdQuery;
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

    public static void main(String args[]) {
        System.out.println("=============start================");
//        testHashAndIndexBuilder();
//        testIndexBuilder();
        testQueryOrder();
        System.out.println("=============end================");
    }
    
    private static void testIndexBuilder() {
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
                storeFolderList.get(0), "0",
                HASH_INDEX_INDEX_ID_NAME, countDownLatch);
        sib.start();
    }
    
    private static void testHashAndIndexBuilder() {
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
        StringHashAndIndexBuilder sib = new StringHashAndIndexBuilder(
                orderFileList, storeFolderList.get(0), REGION_NUMBER,
                HASH_INDEX_INDEX_ID_NAME, countDownLatch);
        sib.start();
//        List<String> orderFileList = new ArrayList<String>();
//        orderFileList.add("order_records.txt");
//        OrderIndexBuilder.build(orderFileList);
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

    private static void testFileWriter() {
        // 测试queryOrder接口，按订单号查找某条记录
        try {
            String encoding = "UTF-8";
            FileUtil.writeFixedBytesLine("test.txt", encoding, "记录123", 16, 1);
            String line = FileUtil.getFixedBytesLine("test.txt", encoding, 16, 1, true);
            System.out.println(line);
            System.out.println("记录123".getBytes(encoding).length);
            System.out.println(line.getBytes(encoding).length);
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println();
    }
}
