package com.alibaba.middleware.race.orderSystemImpl;

import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.BuyerHashFile;
import com.alibaba.middleware.race.file.GoodHashFile;
import com.alibaba.middleware.race.file.OrderHashFile;
import com.alibaba.middleware.race.good.GoodIdQuery;
import com.alibaba.middleware.race.good.IndexFile;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.orderSystemInterface.OrderSystem;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by jiangchao on 2016/7/11.
 */
public class OrderSystemImpl implements OrderSystem {

    //实现无参构造函数
    public OrderSystemImpl() {

    }

    @Override
    public void construct(Collection<String> orderFiles, Collection<String> buyerFiles,
                                    Collection<String> goodFiles, Collection<String> storeFolders)
            throws IOException, InterruptedException {

        //按订单号hash成多个小文件
        OrderHashFile.generateOrderIdHashFile(orderFiles, buyerFiles, goodFiles, null, FileConstant.FILE_NUMS);
        //按买家ID hash成多个小文件
        OrderHashFile.generateBuyerIdHashFile(orderFiles, buyerFiles, goodFiles, null, FileConstant.FILE_NUMS);
        //按商品ID hash成多个小文件
        OrderHashFile.generateGoodIdHashFile(orderFiles, buyerFiles, goodFiles, null, FileConstant.FILE_NUMS);
        //将商品文件hash成多个小文件
        GoodHashFile.generateGoodHashFile(orderFiles, buyerFiles, goodFiles, null, FileConstant.FILE_NUMS);
        //将买家文件hash成多个小文件
        BuyerHashFile.generateBuyerHashFile(orderFiles, buyerFiles, goodFiles, null, FileConstant.FILE_NUMS);

        //根据goodid生成一级二级索引
        IndexFile.generateGoodIdIndex();

        //随机选择了按订单号hash的小文件中的0号文件，将里面的订单记录加载到内存
        //PageCache.cacheOrderIdFile(0);

    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {

        com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
        Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> keyValueMap = new HashMap<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue>();
        int hashIndex = (int) (orderId % 25);
        PageCache.cacheOrderIdFile(hashIndex);
        Order order = PageCache.orderMap.get(orderId);
        if (order == null) {
            System.out.println("要查询的订单数据不再缓存中");
        }
        System.out.println(order);
        keyValueMap.putAll(order.getKeyValues());
        String goodid = order.getKeyValues().get("goodid").getValue();
        String buyerid = order.getKeyValues().get("buyerid").getValue();
        Good good = null;
        Buyer buyer = null;
        if (goodid != null) {
            good = PageCache.goodMap.get(goodid);
            keyValueMap.putAll(good.getKeyValues());
        }
        if (buyerid != null) {
            buyer = PageCache.buyerMap.get(buyerid);
            keyValueMap.putAll(buyer.getKeyValues());
        }
        for (String key : keys) {
            if (keyValueMap.containsKey(key)) {
                result.getKeyValues().put(key, keyValueMap.get(key));
            }
        }
        result.setOrderid(orderId);
        return result;
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        List<com.alibaba.middleware.race.orderSystemImpl.Result> results = new ArrayList<com.alibaba.middleware.race.orderSystemImpl.Result>();
        int hashIndex = (int) (Math.abs(buyerid.hashCode()) % FileConstant.FILE_NUMS);
        PageCache.cacheBuyerIdFile(hashIndex);
        Iterator iter = PageCache.orderMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Long key = (Long) entry.getKey();
            Order val = (Order) entry.getValue();
            if (val.getKeyValues().get("buyerid") != null && val.getKeyValues().get("buyerid").getValue().equals(buyerid)
                    && val.getKeyValues().get("createtime") != null && Long.valueOf(val.getKeyValues().get("createtime").getValue()) >= startTime && Long.valueOf(val.getKeyValues().get("createtime").getValue()) <= endTime) {
                com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
                //加入订单的所有属性kv
                result.setOrderid(key);
                result.setKeyValues(val.getKeyValues());
                //加入对应买家的所有属性kv
                Buyer buyer = PageCache.buyerMap.get(val.getKeyValues().get("buyerid").getValue());
                result.getKeyValues().putAll(buyer.getKeyValues());
                //加入对应商品的所有属性kv
                Good good = PageCache.goodMap.get(val.getKeyValues().get("goodid").getValue());
                result.getKeyValues().putAll(good.getKeyValues());
                results.add(result);
            }
        }


        //对所求结果按照交易时间从大到小排序
        Collections.sort(results, new Comparator<com.alibaba.middleware.race.orderSystemImpl.Result>() {

            @Override public int compare(com.alibaba.middleware.race.orderSystemImpl.Result o1,
                                         com.alibaba.middleware.race.orderSystemImpl.Result o2) {
                return o2.get("createtime").getValue().compareTo(o1.get("createtime").getValue());
            }
        });

        for (com.alibaba.middleware.race.orderSystemImpl.Result result : results) {
            System.out.println(result);
        }

        return results.iterator();
    }

    @Override
    public Iterator<com.alibaba.middleware.race.orderSystemImpl.Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        //flag为1表示查询所有字段
        int flag = 0;
        if (keys == null) {
            flag = 1;
        } else if (goodid == null || keys.size() == 0) {
            return null;
        }
        List<com.alibaba.middleware.race.orderSystemImpl.Result> results = new ArrayList<com.alibaba.middleware.race.orderSystemImpl.Result>();
        int hashIndex = (int) (Math.abs(goodid.hashCode()) % FileConstant.FILE_NUMS);
        //获取goodid的所有订单信息
        List<Order> orders = GoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0) return null;
        Map<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue> keyValueMap = new HashMap<String, com.alibaba.middleware.race.orderSystemImpl.KeyValue>();
        for (Order order : orders) {

            //加入对应买家的所有属性kv
            if (PageCache.buyerMap.get(order.getKeyValues().get("buyerid").getValue()) == null) {
                PageCache.cacheBuyerFile(Math.abs(order.getKeyValues().get("buyerid").getValue().hashCode()) % FileConstant.FILE_NUMS);
            }
            Buyer buyer = PageCache.buyerMap.get(order.getKeyValues().get("buyerid").getValue());
            if (buyer != null && buyer.getKeyValues() != null) {
                keyValueMap.putAll(buyer.getKeyValues());
            }
            //加入对应商品的所有属性kv
            if (PageCache.goodMap.get(order.getKeyValues().get("goodid").getValue()) == null) {
                PageCache.cacheGoodFile(hashIndex);
            }
            Good good = PageCache.goodMap.get(order.getKeyValues().get("goodid").getValue());
            if (good != null && good.getKeyValues() != null) {
                keyValueMap.putAll(good.getKeyValues());
            }
            //加入订单信息的所有属性kv
            keyValueMap.putAll(order.getKeyValues());

            com.alibaba.middleware.race.orderSystemImpl.Result result = new com.alibaba.middleware.race.orderSystemImpl.Result();
            if (flag == 1) {
                result.setKeyValues(keyValueMap);
            } else {
                for (String key : keys) {
                    if (keyValueMap.containsKey(key)) {
                        result.getKeyValues().put(key, keyValueMap.get(key));
                    }
                }
            }
            result.setOrderid(order.getId());
            results.add(result);
        }
        //对所求结果按照交易订单从小到大排序
        Collections.sort(results, new Comparator<com.alibaba.middleware.race.orderSystemImpl.Result>() {

            @Override
            public int compare(com.alibaba.middleware.race.orderSystemImpl.Result o1,
                                         com.alibaba.middleware.race.orderSystemImpl.Result o2) {
                double diff = 0;
                try {
                    diff = (o1.get("amount").valueAsDouble() - o2.get("amount").valueAsDouble());
                } catch (TypeException e) {
                    e.printStackTrace();
                }
                if (diff > 0) {
                    return 1;
                }
                return -1;
            }
        });

        for (com.alibaba.middleware.race.orderSystemImpl.Result result : results) {
            System.out.println(result);
        }

        return results.iterator();
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        if (goodid == null || key == null) return null;
        com.alibaba.middleware.race.orderSystemImpl.KeyValue keyValue = new com.alibaba.middleware.race.orderSystemImpl.KeyValue();
        int hashIndex = (int) (Math.abs(goodid.hashCode()) % FileConstant.FILE_NUMS);
        List<Order> orders = GoodIdQuery.findByGoodId(goodid, hashIndex);
        if (orders == null || orders.size() == 0) return null;
        double value = 0;
        int count = 0;
        for (Order order : orders) {
            //加入订单信息的所有属性kv
            if (order.getKeyValues().containsKey(key)) {
                String str = order.getKeyValues().get(key).getValue();
                if (NumberUtils.isNumber(str)) {
                    value += Double.valueOf(str);
                    count++;
                    continue;
                }
                return null;
            }

            //加入对应买家的所有属性kv
            if (PageCache.buyerMap.get(order.getKeyValues().get("buyerid").getValue()) == null) {
                PageCache.cacheBuyerFile(Math.abs(order.getKeyValues().get("buyerid").getValue().hashCode()) % FileConstant.FILE_NUMS);
            }
            Buyer buyer = PageCache.buyerMap.get(order.getKeyValues().get("buyerid").getValue());
            if (buyer.getKeyValues().containsKey(key)) {
                String str = buyer.getKeyValues().get(key).getValue();
                if (NumberUtils.isNumber(str)) {
                    value += Double.valueOf(str);
                    count++;
                    continue;
                }
                return null;
            }
            //加入对应商品的所有属性kv
            if (PageCache.goodMap.get(order.getKeyValues().get("goodid").getValue()) == null) {
                PageCache.cacheGoodFile(hashIndex);
            }
            Good good = PageCache.goodMap.get(order.getKeyValues().get("goodid").getValue());
            if (good.getKeyValues().containsKey(key)) {
                String str = good.getKeyValues().get(key).getValue();
                if (NumberUtils.isNumber(str)) {
                    value += Double.valueOf(str);
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
        keyValue.setValue(String.valueOf(value));
        return keyValue;
    }

}
