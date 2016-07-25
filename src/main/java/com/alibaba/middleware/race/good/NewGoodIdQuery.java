package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.buyer.BuyerQuery;
import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class NewGoodIdQuery {
    public static List<Order> findByGoodId(String goodId, int index) {
        if (goodId == null) return null;
        List<Order> orders = new ArrayList<Order>();
        try {

            File rankFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_RANK_BY_GOODID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "rw");

            File indexFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_GOODID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = TwoIndexCache.findGoodIdOneIndexPosition(goodId, index);

            //2.查找一级索引
            indexRaf.seek(position);
            String oneIndex = null;
            String onePlusIndex = null;
            int count = 0;
            while ((oneIndex = indexRaf.readLine()) != null) {
                String[] keyValue = oneIndex.split(":");
                if (goodId.equals(keyValue[0])) {
                    break;
                }
                count++;
                if (count >= FileConstant.goodIdIndexRegionSizeMap.get(index)) {
                    return null;
                }
            }
            if (oneIndex == null) return null;
            onePlusIndex = indexRaf.readLine();

            //3.按行读取内容
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
        try {
            File rankFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_RANK_BY_GOODID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "rw");

            File indexFile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_GOODID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = TwoIndexCache.findGoodIdOneIndexPosition(goodId, index);

            //2.查找一级索引
            indexRaf.seek(position);
            String oneIndex = null;
            String onePlusIndex = null;
            int count = 0;
            while ((oneIndex = indexRaf.readLine()) != null) {
                String[] keyValue = oneIndex.split(":");
                if (goodId.equals(keyValue[0])) {
                    break;
                }
                count++;
                if (count >= FileConstant.goodIdIndexRegionSizeMap.get(index)) {
                    return 0;
                }
            }
            if (oneIndex == null) return 0;
            onePlusIndex = indexRaf.readLine();

            //3.按行读取内容
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

    public static Iterator<Result> findOrdersByGood(String salerid, String goodid, Collection<String> keys) {
        List<Result> results = new ArrayList<Result>();
        if (goodid == null) {
            return results.iterator();
        }

        int hashIndex = (int) (Math.abs(goodid.hashCode()) % FileConstant.FILE_NUMS);

        //获取goodid的所有订单信息
        List<Order> orders = NewGoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0) {
            return results.iterator();
        }
        if (keys != null && keys.size() == 0) {
            for (Order order : orders) {
                Result result = new Result();
                result.setOrderid(order.getId());
                results.add(result);
            }
            //对所求结果按照交易订单从小到大排序
            return results.iterator();
        }

        for (Order order : orders) {
            Result result = new Result();
            //加入订单信息的所有属性kv
            if (keys == null) {
                result.getKeyValues().putAll(order.getKeyValues());
            } else {
                for (String key : keys) {
                    if (order.getKeyValues().containsKey(key)) {
                        result.getKeyValues().put(key, order.getKeyValues().get(key));
                    }
                }
            }
            result.setOrderid(order.getId());
            results.add(result);
        }
        return results.iterator();
    }

    public static OrderSystem.KeyValue sumValuesByGood(String goodid, String key) {
        if (goodid == null || key == null) return null;
        KeyValue keyValue = new KeyValue();
        int hashIndex = (int) (Math.abs(goodid.hashCode()) % FileConstant.FILE_NUMS);
        double value = 0;
        long longValue = 0;
        //flag=0表示Long类型，1表示Double类型
        int flag = 0;

        if (KeyCache.goodKeyCache.contains(key)) {
            //加入对应商品的所有属性kv
            int num = NewGoodIdQuery.findOrderNumberByGoodKey(goodid, hashIndex);
            Good good = GoodQuery.findGoodById(goodid, hashIndex);

            if (good == null) return null;
            if (good.getKeyValues().containsKey(key)) {
                String str = good.getKeyValues().get(key).getValue();
                if (flag == 0 && str.contains(".")) {
                    flag = 1;
                }
                if (NewGoodIdQuery.isNumeric(str)) {
                    if (flag == 0) {
                        longValue = num * Long.valueOf(str);
                        keyValue.setKey(key);
                        keyValue.setValue(String.valueOf(longValue));
                    } else {
                        value = num * Double.valueOf(str);
                        keyValue.setKey(key);
                        keyValue.setValue(String.valueOf(value));
                    }
                    return keyValue;
                }
                return null;
            } else {
                return null;
            }
        }

        List<Order> orders = NewGoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0) return null;
        int count = 0;

        for (Order order : orders) {
            //加入订单信息的所有属性kv
            if (order.getKeyValues().containsKey(key)) {
                String str = order.getKeyValues().get(key).getValue();
                if (flag == 0 && str.contains(".")) {
                    flag = 1;
                }
                if (NumberUtils.isNumber(str)) {
                    if (flag == 0) {
                        longValue += Long.valueOf(str);
                        value += Double.valueOf(str);
                    } else {
                        value += Double.valueOf(str);
                    }
                    count++;
                    continue;
                }
                return null;
            }
        }
        if (count == 0) {
            return null;
        }
        keyValue.setKey(key);
        if (flag == 0) {
            keyValue.setValue(String.valueOf(longValue));
        } else {
            keyValue.setValue(String.valueOf(value));
        }
        return keyValue;
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
