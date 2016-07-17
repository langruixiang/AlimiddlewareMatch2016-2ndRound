package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class GoodIdQuery {
    public static List<Order> findByGoodId(String goodId, int index) {
        System.out.println("==========:"+goodId + " index:" + index);
        List<Order> orders = new ArrayList<Order>();
        try {
            FileInputStream twoIndexFile = null;
            twoIndexFile = new FileInputStream(FileConstant.FILE_TWO_INDEXING_BY_GOODID + index);
            BufferedReader twoIndexBR = new BufferedReader(new InputStreamReader(twoIndexFile));

            File hashFile = new File(FileConstant.FILE_INDEX_BY_GOODID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");

            File indexFile = new File(FileConstant.FILE_INDEXING_BY_GOODID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = 0;
            while ((str = twoIndexBR.readLine()) != null) {
                String[] keyValue = str.split(":");
                if (goodId.compareTo(keyValue[0]) < 0) {
                    System.out.println("--------"+keyValue[0]);
                    break;
                } else {
                    position = Long.valueOf(keyValue[1]);
                }
            }

            System.out.println(position);

            //2.查找一级索引
            indexRaf.seek(position);
            String oneIndex = null;
            while ((oneIndex = indexRaf.readLine()) != null) {
                String[] keyValue = oneIndex.split(":");
                if (goodId.equals(keyValue[0])) {
                    break;
                }
            }

            //3.按行读取内容
            String[] keyValue = oneIndex.split(":");
            System.out.println(keyValue[1]);
            String[] positions = keyValue[1].split("\\|");
            //System.out.println("======" + positions.length);
            for (String pos : positions) {
                System.out.println(pos);
                hashRaf.seek(Long.valueOf(pos));
                String orderContent = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
                System.out.println(orderContent);

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
                order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));
                orders.add(order);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orders;
    }

    public static void main(String args[]) {

        //IndexFile.generateGoodIdIndex();
        findByGoodId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
