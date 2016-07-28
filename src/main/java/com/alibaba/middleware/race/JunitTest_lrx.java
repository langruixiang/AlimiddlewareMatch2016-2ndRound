package com.alibaba.middleware.race;

import com.alibaba.middleware.race.buyer.BuyerIdIndexFile;
import com.alibaba.middleware.race.buyer.BuyerIdQuery;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import com.alibaba.middleware.race.orderSystemImpl.Result;
import com.alibaba.middleware.race.util.FileUtil;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class JunitTest_lrx {

    OrderSystem orderSystem = new OrderSystemImpl();

    @Test
    public void testQueryOrder() {
        //测试queryOrder接口，按订单号查找某条记录
        List<String> keys = new ArrayList<String>();
        keys.add("buyerid");
        keys.add("amount");
        keys.add("buyername");
        keys.add("good_name");
        keys.add("description");
        System.out.println("\n测试queryOrder接口，按订单号查找某条记录: ");
        Result result = (Result) orderSystem.queryOrder(609670049, null);
        System.out.println(result.get("buyerid").getValue());
        System.out.println(result.get("amount").getValue());
        System.out.println(result.get("buyername").getValue());
        System.out.println(result.get("good_name").getValue());
        System.out.println(result.get("description").getValue());
        
        result = (Result) orderSystem.queryOrder(609670049, null);
        System.out.println(result.get("buyerid").getValue());
        System.out.println(result.get("amount").getValue());
        System.out.println(result.get("buyername").getValue());
        System.out.println(result.get("good_name").getValue());
        System.out.println(result.get("description").getValue());
        
        result = (Result) orderSystem.queryOrder(612584687, null);
        System.out.println(result.get("buyerid").getValue());
        System.out.println(result.get("amount").getValue());
        System.out.println(result.get("buyername").getValue());
        System.out.println(result.get("good_name").getValue());
        System.out.println(result.get("description").getValue());
    }

    @Test
    public void testQueryOrdersByBuyer() {
        //测试queryOrderByBuyer接口，查找某个买家在某个时间段的所有记录
        System.out.println("\n测试queryOrderByBuyer接口，查找某个买家在某个时间段的所有记录: ");
        Iterator<Result> resultIterator = orderSystem.queryOrdersByBuyer(1462018520, 1473999229, "wx-a0e0-6bda77db73ca");
        while (resultIterator.hasNext()) {
            System.out.println("===============");
            Result result2 = resultIterator.next();
            System.out.println(result2.get("orderid").getValue());
        }
        
        resultIterator = orderSystem.queryOrdersByBuyer(1462018520, 1473999229, "wx-a0e0-6bda77db73ca");
        while (resultIterator.hasNext()) {
            System.out.println("===============");
            Result result2 = resultIterator.next();
            System.out.println(result2.get("orderid").getValue());
        }
        
        resultIterator = orderSystem.queryOrdersByBuyer(1470285742, 1478898941, "tp-a20e-8248d4665332");
        while (resultIterator.hasNext()) {
            System.out.println("===============");
            Result result2 = resultIterator.next();
            System.out.println(result2.get("orderid").getValue());
        }
    }

    @Ignore
    public void testQueryOrdersBySaler() {
        //测试queryOrderBySaler接口，查找某个卖家的某个商品的所有记录信息
        List<String> keys = new ArrayList<String>();
        keys.add("orderid");
//        keys.add("buyerid");
//        keys.add("amount");
        keys.add("buyername");
        System.out.println("\n测试queryOrderBySaler接口，查找某个卖家的某个商品的所有记录信息: ");
        Iterator<Result> resultIterator2 = orderSystem.queryOrdersBySaler("tm-bad2-ec455f2bcbc0", "al-a63c-e1e294d6bcb1", keys);
        while (resultIterator2.hasNext()) {
            Result result3 = resultIterator2.next();
            System.out.println(result3.get("orderid").getValue() + " " + result3.get("buyername"));
        }
        
        resultIterator2 = orderSystem.queryOrdersBySaler("tm-bad2-ec455f2bcbc0", "al-a63c-e1e294d6bcb1", keys);
        while (resultIterator2.hasNext()) {
            Result result3 = resultIterator2.next();
            System.out.println(result3.get("orderid").getValue() + " " + result3.get("buyername"));
        }
        keys.clear();
        
        keys.add("orderid");

        keys.add("address");
        resultIterator2 = orderSystem.queryOrdersBySaler("ay-9cf3-9ba0c6d504a7", "gd-80fa-bc88216aa5be", keys);
        while (resultIterator2.hasNext()) {
            Result result3 = resultIterator2.next();
            System.out.println(result3.get("orderid").getValue() + " " + result3.get("address"));
        }
    }

    @Ignore
    public void testSumOrdersByGood() {
        //测试sumOrdersByGood接口，查找某个商品的某个属性的聚合值
        System.out.println("\n测试sumOrdersByGood接口，查找某个商品的某个属性的聚合值: ");
        KeyValue keyValue = (KeyValue) orderSystem.sumOrdersByGood("goodal_a289ad59-2660-42af-8618-018fd161c391", "amount");
        System.out.println(keyValue.getKey() + ": " + keyValue.getValue());
    }

    @Ignore
    public void testBuyeridIndex() {
        //测试sumOrdersByGood接口，查找某个商品的某个属性的聚合值
        System.out.println("\n测试buyerid生成一级二级索引: ");
        BuyerIdIndexFile buyerIdIndexFile = new BuyerIdIndexFile(null, null, 0);
        buyerIdIndexFile.generateBuyerIdIndex();
        String str = "ap_236ed7ca-dcb9-4562-8b35-072834c45d18";
        int hashIndex = Math.abs(str.hashCode()) % FileConstant.FILE_ORDER_NUMS;
        BuyerIdQuery.findByBuyerId("ap_236ed7ca-dcb9-4562-8b35-072834c45d18", 1463076523, 1465018171, hashIndex);
    }

    static {

        OrderSystem orderSystem = new OrderSystemImpl();
        List<String> orderFileList = new ArrayList<String>();
        orderFileList.add("order.0.0");
        orderFileList.add("order.0.3");
        orderFileList.add("order.1.1");
        orderFileList.add("order.2.2");
//        orderFileList.add("order_records.txt");

        List<String> buyerFileList = new ArrayList<String>();
        buyerFileList.add("buyer.0.0");
        buyerFileList.add("buyer.1.1");
//        buyerFileList.add("buyer_records.txt");

        List<String> goodFileList = new ArrayList<String>();
        goodFileList.add("good.0.0");
        goodFileList.add("good.1.1");
        goodFileList.add("good.2.2");
//        goodFileList.add("good_records.txt");
        
        List<String> storeFolderList = new ArrayList<String>();
        FileUtil.createDir("s1");
        FileUtil.createDir("s2");
        FileUtil.createDir("s3");
        storeFolderList.add("./s1/");
        storeFolderList.add("./s2/");
        storeFolderList.add("./s3/");
        
        try {
            orderSystem.construct(orderFileList, buyerFileList, goodFileList, storeFolderList);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
