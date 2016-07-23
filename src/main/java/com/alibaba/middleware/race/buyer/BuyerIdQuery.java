package com.alibaba.middleware.race.buyer;


import com.alibaba.middleware.race.buyer.BuyerIdIndexFile;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class BuyerIdQuery {
    public static List<Order> findByBuyerId(String buyerId, long starttime, long endtime, int index) {
        if (buyerId == null || buyerId.isEmpty()) return null;
        String beginKey = buyerId + "_" + starttime;
        String endKey = buyerId + "_" + endtime;
        System.out.println("==========:"+buyerId + " index:" + index);
        List<Order> orders = new ArrayList<Order>();
        try {
//            FileInputStream twoIndexFile = null;
//            twoIndexFile = new FileInputStream(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_TWO_INDEXING_BY_BUYERID + index);
//            BufferedReader twoIndexBR = new BufferedReader(new InputStreamReader(twoIndexFile));

//            File hashFile = new File(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_INDEX_BY_BUYERID + index);
//            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");

            File rankFile = new File(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_RANK_BY_BUYERID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "rw");

            File indexFile = new File(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_BUYERID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = TwoIndexCache.findBuyerIdOneIndexPosition(buyerId, starttime, endtime, index);
//            while ((str = twoIndexBR.readLine()) != null) {
//                String[] keyValue = str.split(":");
//                if (endKey.compareTo(keyValue[0]) > 0) {
//                    //System.out.println("--------"+keyValue[0]);
//                    break;
//                } else {
//                    position = Long.valueOf(keyValue[1]);
//                }
//            }

            //System.out.println(position);

            //2.查找一级索引
            long oneIndexStartTime = System.currentTimeMillis();
            int count = 0;
            indexRaf.seek(position);
            String oneIndex = null;
            String oneIndexTmp = null;
            String onePlusIndex = null;
            while ((oneIndexTmp = indexRaf.readLine()) != null) {
                count++;
                String[] keyValue = oneIndexTmp.split(":");
                if (endKey.compareTo(keyValue[0]) <= 0) {
                    continue;
                } else if (beginKey.compareTo(keyValue[0]) > 0) {
                    onePlusIndex = oneIndexTmp;
                    break;
                }
                if (oneIndex == null) {
                    oneIndex = oneIndexTmp;
                }
            }
            if (oneIndex == null) return null;
            System.out.println("===queryOrdersByBuyer===oneindex==buyerid:" + buyerId +  " count: " + count + " time :" + (System.currentTimeMillis() - oneIndexStartTime));

            //3.按行读取内容
            long handleStartTime = System.currentTimeMillis();
            System.out.println(oneIndex);
            String[] keyValue = oneIndex.split(":");
            //System.out.println(keyValue[1]);
            String pos = keyValue[1];
            int length = 0;
            if (onePlusIndex != null) {
                String[] kv = onePlusIndex.split(":");
                System.out.println(kv[1] + ":" + pos);
                length = (int) (Long.valueOf(kv[1]) - Long.valueOf(pos) -1);
            } else {
                System.out.println(hashRaf.length() + ":" + pos);
                length = (int) (hashRaf.length() - Long.valueOf(pos));
            }
            hashRaf.seek(Long.valueOf(pos));
            byte[] bytes = new byte[length];
            hashRaf.read(bytes, 0, length);
            String orderStrs = new String(bytes);
            String[] constents = orderStrs.split("\n");
            for (String orderContent : constents) {
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
            System.out.println("===queryOrdersByBuyer===handle==buyerid:" + buyerId + " size :" + orders.size() + " time :" + (System.currentTimeMillis() - handleStartTime));
            //twoIndexBR.close();
            hashRaf.close();
            indexRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orders;
    }

    public static void main(String args[]) {

        //BuyerIdIndexFile.generateBuyerIdIndex();
        //findByBuyerId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
