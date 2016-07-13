package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import java.io.*;
import java.util.HashMap;
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
    public static Map<String, Buyer> buyerMap = new HashMap<String, Buyer>();

    //存储商品信息
    public static Map<String, Good> goodMap = new HashMap<String, Good>();


    //选择一个订单文件加载到内存中, 将用户文件和商品文件全部加载到内存中
    public static void cacheFile(int index) {

        //清空缓存
        orderMap.clear();
        buyerMap.clear();
        goodMap.clear();

        try {
            FileInputStream order_records = new FileInputStream(FileConstant.FILE_INDEX + index);
            FileInputStream buyer_records = new FileInputStream("buyer_records.txt");
            FileInputStream good_records = new FileInputStream("good_records.txt");
            BufferedReader buyer_br = new BufferedReader(new InputStreamReader(buyer_records));
            BufferedReader good_br = new BufferedReader(new InputStreamReader(good_records));
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

            while ((str = buyer_br.readLine()) != null) {
                Buyer buyer = new Buyer();
                String[] keyValues = str.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] strs = keyValues[i].split(":");
                    KeyValue keyValue =new KeyValue();
                    keyValue.setKey(strs[0]);
                    keyValue.setValue(strs[1]);
                    buyer.getKeyValues().put(strs[0], keyValue);
                }
                buyer.setId(buyer.getKeyValues().get("buyerid").getValue());
                buyerMap.put(buyer.getId(), buyer);
            }
            while ((str = good_br.readLine()) != null) {
                Good good = new Good();
                String[] keyValues = str.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] strs = keyValues[i].split(":");
                    KeyValue keyValue = new KeyValue();
                    keyValue.setKey(strs[0]);
                    keyValue.setValue(strs[1]);
                    good.getKeyValues().put(strs[0], keyValue);
                }
                good.setId(good.getKeyValues().get("goodid").getValue());
                goodMap.put(good.getId(), good);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
