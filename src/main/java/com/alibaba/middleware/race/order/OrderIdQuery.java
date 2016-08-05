package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.buyer.BuyerQuery;
import com.alibaba.middleware.race.cache.FileNameCache;
import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.good.GoodQuery;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import com.alibaba.middleware.race.util.RandomAccessFileUtil;

import java.io.*;
import java.util.*;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class OrderIdQuery {
    public static Order findByOrderId(long orderId, int index) {
        Order order = new Order();
        try {

            File indexFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_ORDERID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "r");

            //1.查找二·级索引
            long position = TwoIndexCache.findOrderIdOneIndexPosition(orderId, index);
            //2.查找一级索引
            String oneIndex = null;
            int count = 0;
            long offset = position;
            while ((oneIndex = RandomAccessFileUtil.readLine(indexRaf, offset)) != null) {
                offset += (oneIndex.getBytes().length + 1);
                String[] keyValue = oneIndex.split(":");
                if (orderId == Long.valueOf(keyValue[0])) {
                    break;
                }
                count++;
                if (count >= FileConstant.orderIdIndexRegionSizeMap.get(index)) {
                    indexRaf.close();
                    return null;
                }
            }
            indexRaf.close();
            //3.按行读取内容
            String[] keyValue = oneIndex.split(":");
            String srcFile = keyValue[1];
            long pos = Long.valueOf(keyValue[2]);

            File hashFile = new File(FileNameCache.fileNameMap.get(Integer.valueOf(srcFile)));
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "r");
            String orderContent = oneIndex = RandomAccessFileUtil.readLine(hashRaf, Long.valueOf(pos));

            //4.将字符串转成order对象集合
            StringTokenizer stringTokenizer = new StringTokenizer(orderContent, "\t");
            while (stringTokenizer.hasMoreElements()) {
                StringTokenizer kvalue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                String key = kvalue.nextToken();
                String value = kvalue.nextToken();
                KeyValue kv = new KeyValue();
                kv.setKey(key);
                kv.setValue(value);
                order.getKeyValues().put(key, kv);
            }
            order.setId(orderId);
            hashRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return order;
    }

    public static OrderSystem.Result findOrder(long orderId, Collection<String> keys) {
        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
        int hashIndex = (int) (orderId % FileConstant.FILE_ORDER_NUMS);
        Order order = OrderIdQuery.findByOrderId(orderId, hashIndex);
        List<String> maybeOrderSearchKeys = new ArrayList<String>();
        List<String> goodSearchKeys = new ArrayList<String>();
        List<String> buyerSearchKeys = new ArrayList<String>();
        if (keys != null) {
            for (String key : keys) {
                if (KeyCache.goodKeyCache.containsKey(key)) {
                    goodSearchKeys.add(key);
                } else if (KeyCache.buyerKeyCache.containsKey(key)) {
                    buyerSearchKeys.add(key);
                } else {
                    maybeOrderSearchKeys.add(key);
                }
            }
        }
        if (order == null) {
            return null;
        }
        if (keys != null && keys.isEmpty()) {
            result.setOrderid(orderId);
            return result;
        }
        {
            if (keys == null || buyerSearchKeys.size() > 0) {
                String buyerId = order.getKeyValues().get("buyerid").getValue();
                //加入对应买家的所有属性kv
                Buyer buyer = BuyerQuery.findBuyerById(buyerId);

                if (buyer != null && buyer.getKeyValues() != null) {
                    result.getKeyValues().putAll(buyer.getKeyValues());
                }
            }
        }

        {
            if (keys == null || goodSearchKeys.size() > 0) {
                String goodId = order.getKeyValues().get("goodid").getValue();
                //加入对应商品的所有属性kv
                Good good = GoodQuery.findGoodById(goodId);

                if (good != null && good.getKeyValues() != null) {
                    result.getKeyValues().putAll(good.getKeyValues());
                }
            }

        }
        if (keys == null) {
            result.getKeyValues().putAll(order.getKeyValues());
        } else {
            for (String key : maybeOrderSearchKeys) {
                if (order.getKeyValues().containsKey(key)) {
                    result.getKeyValues().put(key, order.getKeyValues().get(key));
                }
            }
        }
        result.setOrderid(orderId);
        return result;
    }

}
