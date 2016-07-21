package com.alibaba.middleware.race.order;

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
public class OrderIdQuery {
    public static Order findByOrderId(long orderId, int index) {
        //System.out.println("==========:"+goodId + " index:" + index);
        Order order = new Order();
        try {
            FileInputStream twoIndexFile = new FileInputStream(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_TWO_INDEXING_BY_ORDERID + index);
            BufferedReader twoIndexBR = new BufferedReader(new InputStreamReader(twoIndexFile));

            File hashFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_INDEX_BY_ORDERID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");

            File indexFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_ORDERID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = 0;
            while ((str = twoIndexBR.readLine()) != null) {
                String[] keyValue = str.split(":");
                System.out.println(keyValue[0]);
                if (orderId < Long.valueOf(keyValue[0])) {
                    //System.out.println("--------"+keyValue[0]);
                    break;
                } else {
                    position = Long.valueOf(keyValue[1]);
                }
            }

            //System.out.println(position);

            //2.查找一级索引
            indexRaf.seek(position);
            String oneIndex = null;
            int count = 0;
            while ((oneIndex = indexRaf.readLine()) != null) {
                String[] keyValue = oneIndex.split(":");
                if (orderId == Long.valueOf(keyValue[0])) {
                    break;
                }
                count++;
                if (count >= FileConstant.orderIdIndexRegionSizeMap.get(index)) {
                    return null;
                }
            }

            //3.按行读取内容
            String[] keyValue = oneIndex.split(":");
            //System.out.println(keyValue[1]);

            long pos = Long.valueOf(keyValue[1]);
            //System.out.println(pos);
            hashRaf.seek(Long.valueOf(pos));
            String orderContent = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
            //System.out.println(orderContent);

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
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return order;
    }

    public static void main(String args[]) {

        //OrderIdIndexFile.generateGoodIdIndex();
        //findByOrderId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
