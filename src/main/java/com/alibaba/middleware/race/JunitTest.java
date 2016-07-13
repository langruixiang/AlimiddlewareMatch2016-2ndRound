package com.alibaba.middleware.race;

import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import com.alibaba.middleware.race.orderSystemImpl.OrderSystemImpl;
import com.alibaba.middleware.race.orderSystemImpl.Result;
import com.alibaba.middleware.race.orderSystemInterface.OrderSystem;
import org.junit.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class JunitTest {

    OrderSystem orderSystem = new OrderSystemImpl();

    @Test
    public void testQueryOrder() {
        //测试queryOrder接口，按订单号查找某条记录
        List<String> keys = new ArrayList<String>();
        keys.add("buyerid");
        keys.add("amount");
        System.out.println("\n测试queryOrder接口，按订单号查找某条记录: ");
        Result result = (Result) orderSystem.queryOrder(2982138, keys);
        System.out.println(result.get("buyerid").getValue());
    }

    @Test
    public void testQueryOrdersByBuyer() {
        //测试queryOrderByBuyer接口，查找某个买家在某个时间段的所有记录
        System.out.println("\n测试queryOrderByBuyer接口，查找某个买家在某个时间段的所有记录: ");
        Iterator<Result> resultIterator = orderSystem.queryOrdersByBuyer(1463056100, 1463056200, "tb_bd6fd52b-92f0-48ac-91c9-e0bbcbebe6d2");
        while (resultIterator.hasNext()) {
            Result result2 = resultIterator.next();
            System.out.println(result2.get("orderid").getValue());
        }
    }

    @Test
    public void testQueryOrdersBySaler() {
        //测试queryOrderBySaler接口，查找某个卖家的某个商品的所有记录信息
        List<String> keys = new ArrayList<String>();
        keys.add("buyerid");
        keys.add("amount");
        System.out.println("\n测试queryOrderBySaler接口，查找某个卖家的某个商品的所有记录信息: ");
        Iterator<Result> resultIterator2 = orderSystem.queryOrdersBySaler("", "goodal_a289ad59-2660-42af-8618-018fd161c391", keys);
        while (resultIterator2.hasNext()) {
            Result result3 = resultIterator2.next();
            System.out.println(result3.get("buyerid").getValue());
        }
    }

    @Test
    public void testSumOrdersByGood() {
        //测试sumOrdersByGood接口，查找某个商品的某个属性的聚合值
        System.out.println("\n测试sumOrdersByGood接口，查找某个商品的某个属性的聚合值: ");
        KeyValue keyValue = (KeyValue) orderSystem.sumOrdersByGood("goodal_a289ad59-2660-42af-8618-018fd161c391", "amount");
        System.out.println(keyValue.getKey() + ": " + keyValue.getValue());
    }

    static {

        OrderSystem orderSystem = new OrderSystemImpl();
        List<String> orderFileList = new ArrayList<String>();
        orderFileList.add("order_records.txt");

        List<String> buyerFileList = new ArrayList<String>();
        buyerFileList.add("buyer_records.txt");

        List<String> goodFileList = new ArrayList<String>();
        goodFileList.add("good_records.txt");
        try {
            orderSystem.construct(orderFileList, buyerFileList, goodFileList, null);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
