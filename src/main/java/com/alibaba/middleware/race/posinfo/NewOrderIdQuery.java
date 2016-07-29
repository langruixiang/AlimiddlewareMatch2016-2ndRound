package com.alibaba.middleware.race.posinfo;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.model.PosInfo;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class NewOrderIdQuery {
    public static Order findByOrderId(long orderId, int index) {
        Order order = new Order();
        try {

            //1.查找二·级索引
            long position = TwoIndexCache.findOrderIdOneIndexPosition(orderId, index);
            //2.查找一级索引
            String oneIndex = null;
            File indexFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_ORDERID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "r");
            long length = indexRaf.length();
            int count = 0;
            while (position < length) {
                oneIndex = RandomAccessFileUtil.readLine(indexRaf, position);
                position += oneIndex.getBytes().length + 1;
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

            PosInfo posInfo = PosInfo.parseFromString(keyValue[1]);

            File hashFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_INDEX_BY_ORDERID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "r");

            byte[] bytes = new byte[posInfo.length];
            hashRaf.seek(posInfo.offset);
            hashRaf.read(bytes);
            String orderContent = new String(bytes);

            //4.将字符串转成order对象集合
            String[] keyValues = orderContent.split("\t");
            for (int i = 0; i < keyValues.length; i++) {
                String[] strs = keyValues[i].split(":");
                KeyValue kv = new KeyValue();
                kv.setKey(strs[0]);
                kv.setValue(strs[1]);
                order.getKeyValues().put(strs[0], kv);
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
        Order order = NewOrderIdQuery.findByOrderId(orderId, hashIndex);
        List<String> orderSearchKeys = new ArrayList<String>();
        List<String> goodSearchKeys = new ArrayList<String>();
        List<String> buyerSearchKeys = new ArrayList<String>();
        if (keys != null) {
            for (String key : keys) {
                if (KeyCache.orderKeyCache.containsKey(key)) {
                    orderSearchKeys.add(key);
                } else if (KeyCache.goodKeyCache.containsKey(key)) {
                    goodSearchKeys.add(key);
                } else if (KeyCache.buyerKeyCache.containsKey(key)) {
                    buyerSearchKeys.add(key);
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
                int buyerHashIndex = (int) (Math.abs(buyerId.hashCode()) % FileConstant.FILE_BUYER_NUMS);
                //加入对应买家的所有属性kv
                Buyer buyer = NewBuyerQuery.findBuyerById(buyerId, buyerHashIndex);

                if (buyer != null && buyer.getKeyValues() != null) {
                    if (keys ==  null) {
                        result.getKeyValues().putAll(buyer.getKeyValues());
                    } else {
                        Map<String, KeyValue> buyerKeyValues = buyer.getKeyValues();
                        for (String key : buyerSearchKeys) {
                            if (buyerKeyValues.containsKey(key)) {
                                result.getKeyValues().put(key, buyerKeyValues.get(key));
                            }
                        }
                    }
                }
            }
        }

        {
            if (keys == null || goodSearchKeys.size() > 0) {
                String goodId = order.getKeyValues().get("goodid").getValue();
                //加入对应商品的所有属性kv
                int goodIdHashIndex = (int) (Math.abs(goodId.hashCode()) % FileConstant.FILE_GOOD_NUMS);
                Good good = NewGoodQuery.findGoodById(goodId, goodIdHashIndex);

                if (good != null && good.getKeyValues() != null) {
                    if (keys ==  null) {
                        result.getKeyValues().putAll(good.getKeyValues());
                    } else {
                        Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> goodKeyValues = good.getKeyValues();
                        for (String key : goodSearchKeys) {
                            if (goodKeyValues.containsKey(key)) {
                                result.getKeyValues().put(key, goodKeyValues.get(key));
                            }
                        }
                    }
                }
            }

        }
        if (keys == null) {
            result.getKeyValues().putAll(order.getKeyValues());
        } else {
            for (String key : orderSearchKeys) {
                if (order.getKeyValues().containsKey(key)) {
                    result.getKeyValues().put(key, order.getKeyValues().get(key));
                }
            }
        }
        result.setOrderid(orderId);
        return result;
    }

    public static void main(String args[]) {

        //OrderIdIndexFile.generateGoodIdIndex();
        //findByOrderId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
