package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class GoodIdQuery {
    public static List<Order> findByGoodId(String goodId, int index) {
        if (goodId == null) return null;
        System.out.println("==========:"+goodId + " index:" + index);
        List<Order> orders = new ArrayList<Order>();
        try {
//            FileInputStream twoIndexFile = new FileInputStream(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_TWO_INDEXING_BY_GOODID + index);
//            BufferedReader twoIndexBR = new BufferedReader(new InputStreamReader(twoIndexFile));

//            File hashFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + index);
//            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");

            File rankFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_RANK_BY_GOODID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "rw");

            File indexFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_GOODID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long twoIndexStartTime = System.currentTimeMillis();
            long position = TwoIndexCache.findGoodIdOneIndexPosition(goodId, index);
            if (position == -1L) return null;
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
            String onePlusIndex = null;
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
            if (oneIndex == null) return null;
            onePlusIndex = indexRaf.readLine();
            System.out.println("===queryOrdersBySaler===oneindex==goodid:" + goodId +  " count: " + count + " time :" + (System.currentTimeMillis() - oneIndexStartTime));

            //3.按行读取内容
            long handleStartTime = System.currentTimeMillis();
            System.out.println(oneIndex);
            String[] keyValue = oneIndex.split(":");
            //System.out.println(keyValue[1]);
            String pos = keyValue[1];
            //System.out.println("======" + positions.length);
            int length = 0;
            if (onePlusIndex != null) {
                String[] kv = onePlusIndex.split(":");
                length = (int) (Long.valueOf(kv[1]) - Long.valueOf(pos) -1);
            } else {
                length = (int) (hashRaf.length() - Long.valueOf(pos));
            }

            hashRaf.seek(Long.valueOf(pos));
//            while ((orderStr = hashRaf.readLine()) != null) {
//                String orderContent = new String(orderStr.getBytes("iso-8859-1"), "UTF-8");
//                Order order = new Order();
//                String[] keyValues = orderContent.split("\t");
//                for (int i = 0; i < keyValues.length; i++) {
//                    String[] strs = keyValues[i].split(":");
//                    KeyValue kv = new KeyValue();
//                    kv.setKey(strs[0]);
//                    kv.setValue(strs[1]);
//                    order.getKeyValues().put(strs[0], kv);
//                }
//                if (order.getKeyValues().get("goodid").getValue() != null && !goodId.equals(order.getKeyValues().get("goodid").getValue())) {
//                    break;
//                }
//                if (order.getKeyValues().get("orderid").getValue() != null && NumberUtils.isNumber(order.getKeyValues().get("orderid").getValue())){
//                    order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));
//                }
//                orders.add(order);
//            }

            byte[] bytes = new byte[length];
            hashRaf.read(bytes, 0, length);
            String orderStrs = new String(bytes);
            String[] constents = orderStrs.split("\n");
            for (String content : constents) {
                Order order = new Order();
                String[] keyValues = content.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] strs = keyValues[i].split(":");
                    KeyValue kv = new KeyValue();
                    kv.setKey(strs[0]);
                    kv.setValue(strs[1]);
                    order.getKeyValues().put(strs[0], kv);
                }
                if (order.getKeyValues().get("goodid").getValue() != null && !goodId.equals(order.getKeyValues().get("goodid").getValue())) {
                    break;
                }
                if (order.getKeyValues().get("orderid").getValue() != null && NumberUtils.isNumber(order.getKeyValues().get("orderid").getValue())){
                    order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));
                }
                orders.add(order);
            }
            //System.out.println(constents);

            System.out.println("===queryOrdersBySaler===handle==goodid:" + goodId + " size :" + orders.size() + " time :" + (System.currentTimeMillis() - handleStartTime));
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

    public static int findOrderNumberByGoodKey(String goodId, int index) {
        if (goodId == null) return 0;
        System.out.println("==========:"+goodId + " index:" + index);
        try {
            File rankFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_RANK_BY_GOODID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "rw");

            File indexFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_GOODID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long twoIndexStartTime = System.currentTimeMillis();
            long position = TwoIndexCache.findGoodIdOneIndexPosition(goodId, index);
            System.out.println("===queryOrdersBySaler===twoindex==goodid:" + goodId + " time :" + (System.currentTimeMillis() - twoIndexStartTime));

            //2.查找一级索引
            long oneIndexStartTime = System.currentTimeMillis();
            indexRaf.seek(position);
            String oneIndex = null;
            String onePlusIndex = null;
            int count = 0;
            while ((oneIndex = indexRaf.readLine()) != null) {
                String[] keyValue = oneIndex.split(":");
                if (goodId.equals(keyValue[0])) {
                    //System.out.println(oneIndex);
                    break;
                }
                count++;
                if (count >= FileConstant.goodIdIndexRegionSizeMap.get(index)) {
                    return 0;
                }
            }
            if (oneIndex == null) return 0;
            onePlusIndex = indexRaf.readLine();
            System.out.println("===queryOrdersBySaler===oneindex==goodid:" + goodId +  " count: " + count + " time :" + (System.currentTimeMillis() - oneIndexStartTime));

            //3.按行读取内容
            long handleStartTime = System.currentTimeMillis();
            System.out.println(oneIndex);
            String[] keyValue = oneIndex.split(":");
            String pos = keyValue[1];
            int length = 0;
            if (onePlusIndex != null) {
                String[] kv = onePlusIndex.split(":");
                length = (int) (Long.valueOf(kv[1]) - Long.valueOf(pos) -1);
            } else {
                length = (int) (hashRaf.length() - Long.valueOf(pos));
            }

            hashRaf.seek(Long.valueOf(pos));

            byte[] bytes = new byte[length];
            hashRaf.read(bytes, 0, length);
            String orderStrs = new String(bytes);
            String[] constents = orderStrs.split("\n");
            System.out.println("===queryOrdersBySaler===handle==goodid:" + goodId + " size :" + constents.length + " time :" + (System.currentTimeMillis() - handleStartTime));

            hashRaf.close();
            indexRaf.close();
            return constents.length;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static boolean isNumeric(String str){
        //Pattern pattern = Pattern.compile("-?[0-9]+.?[0-9]+");
        Pattern pattern = Pattern.compile("^[-+]?\\d+(\\.\\d+)?$");
        Matcher isNum = pattern.matcher(str);
        if( !isNum.matches() ){
            return false;
        }
        return true;
    }

    public static void main(String args[]) {

        //OrderIdIndexFile.generateGoodIdIndex();
        findByGoodId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
