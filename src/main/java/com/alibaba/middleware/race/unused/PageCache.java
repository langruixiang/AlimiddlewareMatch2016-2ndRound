package com.alibaba.middleware.race.unused;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.KeyValue;
import com.alibaba.middleware.race.model.Order;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by jiangchao on 2016/7/12.
 */
public class PageCache {

    //当前缓存中的页号
    public static int pageIndex;

    //存储订单信息
    public static Map<Long, Order> orderMap = new HashMap<Long, Order>();

    //存储买家信息
    //public static Map<String, Buyer> buyerMap = new HashMap<String, Buyer>();
    public static volatile Map<Integer, Map<String, Buyer>> buyerMap = new LinkedHashMap<Integer, Map<String, Buyer>>(Config.MAX_CONCURRENT, 1) {
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > Config.MAX_CONCURRENT;
        }
    };
    //存储商品信息
    //public static Map<String, Good> goodMap = new HashMap<String, Good>();
    public static volatile Map<Integer, Map<String, Good>> goodMap = new LinkedHashMap<Integer, Map<String, Good>>(Config.MAX_CONCURRENT, 1) {
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > Config.MAX_CONCURRENT;
        }
    };



//    //选择hash后的一个用户文件加载到内存中
//    public static void unusedCacheBuyerFile(int index) {
//
//        //清空缓存
//        Map<String, Buyer> segmentBuyerMap = new HashMap<String, Buyer>();
//        try {
//            FileInputStream buyer_records = new FileInputStream(Config.FIRST_DISK_PATH + FileConstant.FILE_BUYER_HASH + index);
//            BufferedReader buyer_br = new BufferedReader(new InputStreamReader(buyer_records));
//
//            String str = null;
//
//            while ((str = buyer_br.readLine()) != null) {
//                Buyer buyer = new Buyer();
//                String[] keyValues = str.split("\t");
//                for (int i = 0; i < keyValues.length; i++) {
//                    String[] strs = keyValues[i].split(":");
//                    KeyValue keyValue =new KeyValue();
//                    keyValue.setKey(strs[0]);
//                    keyValue.setValue(strs[1]);
//                    buyer.getKeyValues().put(strs[0], keyValue);
//                }
//                buyer.setId(buyer.getKeyValues().get("buyerid").getValue());
//                segmentBuyerMap.put(buyer.getId(), buyer);
//            }
//            buyerMap.put(index, segmentBuyerMap);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    //选择hash后的一个商品文件加载到内存中
//    public static void unusedCacheGoodFile(int index) {
//
//        //清空缓存
//        Map<String, Good> segmentGoodMap = new HashMap<String, Good>();
//        try {
//            FileInputStream good_records = new FileInputStream(Config.FIRST_DISK_PATH + FileConstant.FILE_GOOD_HASH + index);
//            BufferedReader good_br = new BufferedReader(new InputStreamReader(good_records));
//
//            String str = null;
//
//            while ((str = good_br.readLine()) != null) {
//                Good good = new Good();
//                String[] keyValues = str.split("\t");
//                for (int i = 0; i < keyValues.length; i++) {
//                    String[] strs = keyValues[i].split(":");
//                    KeyValue keyValue = new KeyValue();
//                    keyValue.setKey(strs[0]);
//                    keyValue.setValue(strs[1]);
//                    good.getKeyValues().put(strs[0], keyValue);
//                }
//                good.setId(good.getKeyValues().get("goodid").getValue());
//                segmentGoodMap.put(good.getId(), good);
//            }
//            goodMap.put(index, segmentGoodMap);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    //选择按订单号hash后的一个订单文件加载到内存中
    public static void cacheOrderIdFile(int index) {

        //清空缓存
        orderMap.clear();

        try {
            FileInputStream order_records = new FileInputStream(FileConstant.UNSORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX + index);
            BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

            String str = null;
            while ((str = order_br.readLine()) != null) {
                Order order = new Order();
                String[] keyValues = str.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] strs = keyValues[i].split(":");
                    KeyValue keyValue = new KeyValue();
                    keyValue.setKey(strs[0]);
                    keyValue.setValue(strs[1]);
                    order.getKeyValues().put(strs[0], keyValue);
                }
                order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));
                orderMap.put(order.getId(), order);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //选择按买家ID hash后的一个订单文件加载到内存中
    public static void cacheBuyerIdFile(int index) {

        //清空缓存
        orderMap.clear();

        try {
            FileInputStream order_records = new FileInputStream(FileConstant.UNSORTED_BUYER_ID_ONE_INDEX_FILE_PREFIX + index);
            BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

            String str = null;
            while ((str = order_br.readLine()) != null) {
                Order order = new Order();
                String[] keyValues = str.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] strs = keyValues[i].split(":");
                    KeyValue keyValue = new KeyValue();
                    keyValue.setKey(strs[0]);
                    keyValue.setValue(strs[1]);
                    order.getKeyValues().put(strs[0], keyValue);
                }
                order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));
                orderMap.put(order.getId(), order);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //选择按商品ID hash后的一个订单文件加载到内存中
    public static void cacheGoodIdFile(int index) {

        //清空缓存
        orderMap.clear();

        try {
            FileInputStream order_records = new FileInputStream(FileConstant.UNSORTED_GOOD_ID_HASH_FILE_PREFIX + index);
            BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

            String str = null;
            while ((str = order_br.readLine()) != null) {
                Order order = new Order();
                String[] keyValues = str.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] strs = keyValues[i].split(":");
                    KeyValue keyValue = new KeyValue();
                    keyValue.setKey(strs[0]);
                    keyValue.setValue(strs[1]);
                    order.getKeyValues().put(strs[0], keyValue);
                }
                order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));
                orderMap.put(order.getId(), order);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
