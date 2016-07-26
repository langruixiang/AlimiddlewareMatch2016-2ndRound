package com.alibaba.middleware.race.buyer;

import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.good.GoodQuery;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import com.alibaba.middleware.race.orderSystemImpl.Result;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class OldBuyerIdQuery {
    public static List<Order> findByBuyerId(String buyerId, long starttime, long endtime, int index) {
        if (buyerId == null || buyerId.isEmpty()) return null;
        String beginKey = buyerId + "_" + starttime;
        String endKey = buyerId + "_" + endtime;
        System.out.println("==========:"+buyerId + " index:" + index);
        List<Order> orders = new ArrayList<Order>();
        try {

            File hashFile = new File(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_INDEX_BY_BUYERID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");

            File indexFile = new File(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_BUYERID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = TwoIndexCache.findBuyerIdOneIndexPosition(buyerId, starttime, endtime, index);

            //2.查找一级索引
            int count = 0;
            indexRaf.seek(position);
            String oneIndex = null;
            List<String> oneIndexs = new ArrayList<String>();
            while ((oneIndex = indexRaf.readLine()) != null) {
                count++;
                String[] keyValue = oneIndex.split(":");
                if (endKey.compareTo(keyValue[0]) <= 0) {
                    continue;
                } else if (beginKey.compareTo(keyValue[0]) > 0) {
                    break;
                }
                oneIndexs.add(oneIndex);
            }

            //3.按行读取内容
            List<String> orderContents = new ArrayList<String>();
            for (String line : oneIndexs) {
                String[] keyValue = line.split(":");
                String[] positions = keyValue[1].split("\\|");
                for (String pos : positions) {
                    hashRaf.seek(Long.valueOf(pos));
                    String orderContent = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
                    orderContents.add(orderContent);
                    //System.out.println(orderContent);
                }
            }

            for (String orderContent : orderContents) {
                //4.将字符串转成order对象集合
                Order order = new Order();
                String[] keyValues = orderContent.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] strs = keyValues[i].split(":");
                    KeyValue kv = new KeyValue();
                    kv.setKey(strs[0]);
                    kv.setValue(strs[1]);
                    order.getKeyValues().put(strs[0], kv);
                }
                if (order.getKeyValues().get("orderid").getValue() != null && NumberUtils.isNumber(order.getKeyValues().get("orderid").getValue())){
                    order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));
                }
                orders.add(order);
            }
            hashRaf.close();
            indexRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orders;
    }

    public static Iterator<Result> findOrdersByBuyer(long startTime, long endTime, String buyerid) {
        //System.out.println("===queryOrdersByBuyer=====buyerid:" + buyerid + "======starttime:" + startTime + "=========endtime:" + endTime);
        long starttime = System.currentTimeMillis();
        List<Result> results = new ArrayList<Result>();
        int hashIndex = (int) (Math.abs(buyerid.hashCode()) % FileConstant.FILE_ORDER_NUMS);

        int buyerHashIndex = (int) (Math.abs(buyerid.hashCode()) % FileConstant.FILE_BUYER_NUMS);
        Buyer buyer = BuyerQuery.findBuyerById(buyerid, buyerHashIndex);
        if (buyer == null) return results.iterator();

        //获取goodid的所有订单信息
        List<Order> orders = OldBuyerIdQuery.findByBuyerId(buyerid, startTime, endTime, hashIndex);
        if (orders == null || orders.size() == 0) return results.iterator();

        for (Order order : orders) {
            //System.out.println("queryOrdersByBuyer buyerid:"+ buyerid +" : " + order_old.toString());
            Result result = new Result();
            //加入对应买家的所有属性kv
            if (buyer != null && buyer.getKeyValues() != null) {
                result.getKeyValues().putAll(buyer.getKeyValues());
            }
            //加入对应商品的所有属性kv
            int goodIdHashIndex = (int) (Math.abs(order.getKeyValues().get("goodid").getValue().hashCode()) % FileConstant.FILE_GOOD_NUMS);
            Good good = GoodQuery.findGoodById(order.getKeyValues().get("goodid").getValue(), goodIdHashIndex);

            if (good != null && good.getKeyValues() != null) {
                result.getKeyValues().putAll(good.getKeyValues());
            }
            //加入订单信息的所有属性kv
            result.getKeyValues().putAll(order.getKeyValues());
            result.setOrderid(order.getId());
            results.add(result);
        }
        System.out.println("queryOrdersByBuyer :" + buyerid + " time :" + (System.currentTimeMillis() - starttime));
        return results.iterator();
    }

    public static void main(String args[]) {

        //BuyerIdIndexFile.generateBuyerIdIndex();
        //findByBuyerId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
