package com.alibaba.middleware.race.good;

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
public class GoodIdQuery {
    public static List<Order> findByGoodId(String goodId, int index) {
        if (goodId == null) return null;
        //System.out.println("==========:"+goodId + " index:" + index);
        List<Order> orders = new ArrayList<Order>();
        try {
//            FileInputStream twoIndexFile = new FileInputStream(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_TWO_INDEXING_BY_GOODID + index);
//            BufferedReader twoIndexBR = new BufferedReader(new InputStreamReader(twoIndexFile));

            File hashFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");

            File indexFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_GOODID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long twoIndexStartTime = System.currentTimeMillis();
            long position = TwoIndexCache.findGoodIdOneIndexPosition(goodId, index);
            System.out.println("===queryOrdersBySaler===twoindex==goodid:" + goodId + " time :" + (System.currentTimeMillis() - twoIndexStartTime));
//            while ((str = twoIndexBR.readLine()) != null) {
//                String[] keyValue = str.split(":");
//                if (goodId.compareTo(keyValue[0]) < 0) {
//                    //System.out.println("--------"+keyValue[0]);
//                    break;
//                } else {
//                    position = Long.valueOf(keyValue[1]);
//                }
//            }

            //System.out.println(position);

            //2.查找一级索引
            long oneIndexStartTime = System.currentTimeMillis();
            indexRaf.seek(position);
            String oneIndex = null;
            int count = 0;
            while ((oneIndex = indexRaf.readLine()) != null) {
                String[] keyValue = oneIndex.split(":");
                if (goodId.equals(keyValue[0])) {
                    //System.out.println(oneIndex);
                    break;
                }
                count++;
                if (count >= FileConstant.goodIdIndexRegionSizeMap.get(index)) {
                    return null;
                }
            }
            System.out.println("===queryOrdersBySaler===oneindex==goodid:" + goodId +  " count: " + count + " time :" + (System.currentTimeMillis() - oneIndexStartTime));

            //3.按行读取内容
            long handleStartTime = System.currentTimeMillis();
            String[] keyValue = oneIndex.split(":");
            //System.out.println(keyValue[1]);
            String[] positions = keyValue[1].split("\\|");
            //System.out.println("======" + positions.length);
            for (String pos : positions) {
                //System.out.println(pos);
                long SeekStartTime = System.currentTimeMillis();
                hashRaf.seek(Long.valueOf(pos));
                String orderContent = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
                System.out.println("===queryOrdersBySaler===seekposition==goodid:" + goodId +  " time :" + (System.currentTimeMillis() - SeekStartTime));
                //System.out.println(orderContent);

                //4.将字符串转成order对象集合
                long objectStartTime = System.currentTimeMillis();
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
                System.out.println("===queryOrdersBySaler===map object=goodid:" + goodId +  " time :" + (System.currentTimeMillis() - objectStartTime));
                //System.out.println(order);
                orders.add(order);
            }
            System.out.println("===queryOrdersBySaler===handle==goodid:" + goodId + " time :" + (System.currentTimeMillis() - handleStartTime));
//            twoIndexBR.close();
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

        //OrderIdIndexFile.generateGoodIdIndex();
        findByGoodId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
