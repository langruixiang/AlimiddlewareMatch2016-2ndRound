/**
 * TestOrderIndexAndQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.orderSystemImpl.Result;

/**
 * @author wangweiwei
 *
 */
public class TestOrderIndexAndQuery {
    
    public static void main(String args[]) {
        System.out.println("=============start================");

//        testOrderIndexBuilder();
        testQueryOrder();

        System.out.println("=============end================");
    }

    private static void testOrderIndexBuilder() {
        List<String> orderFileList = new ArrayList<String>();
        orderFileList.add("order_records.txt");
        OrderIndexBuilder.build(orderFileList);
    }
    
    private static void testQueryOrder() {
        // 测试queryOrder接口，按订单号查找某条记录
        System.out.println("测试queryOrder接口，按订单号查找某条记录: ");
        OrderQuery orderQuery = new OrderQuery();
        List<String> keys = new ArrayList<String>();
        keys.add("buyerid");
        keys.add("amount");
        keys.add("app_order_76_0");
        Result result = (Result) orderQuery.queryOrder(2982138, keys);
        System.out.println(result.get("buyerid").getValue());
        System.out.println(result.get("amount").getValue());
        System.out.println(result.get("app_order_76_0").getValue());
    }
}
