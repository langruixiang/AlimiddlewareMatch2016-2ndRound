/**
 * TestOrderIdIndex.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.Result;

/**
 * @author wangweiwei
 *
 */
public class TestOrderIdIndex {

    public static void main(String args[]) {
        try {
            System.out.println("=============start================");
            testConstruct();
            testQueryOrder();
            System.out.println("=============end================");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * @throws InterruptedException 
     * 
     */
    private static void testConstruct() throws InterruptedException {
        List<String> orderFileList = new ArrayList<String>();
        orderFileList.add("order_records.txt");
        CountDownLatch hashLatch = new CountDownLatch(1);
        NewOrderIdIndexHash indexHash = new NewOrderIdIndexHash(orderFileList,hashLatch);
        indexHash.start();
        
        CountDownLatch builderCountDownLatch = new CountDownLatch(1);
        NewOrderIdIndexBuilder indexBuilder = new NewOrderIdIndexBuilder(hashLatch, builderCountDownLatch);
        indexBuilder.start();
        builderCountDownLatch.await();
    }

    private static void testQueryOrder() {
        System.out.println("测试queryOrder接口，按订单号查找某条记录: ");
        List<String> keys = new ArrayList<String>();
        keys.add("buyerid");
        keys.add("amount");

        Order order = NewOrderIdQuery.queryOrder(2982270, keys);
        System.out.println(order);
    }
    

    private static void testStringLong() {
        
    }
    
}
