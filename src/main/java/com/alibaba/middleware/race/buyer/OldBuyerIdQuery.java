package com.alibaba.middleware.race.buyer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.OrderIndex;
import com.alibaba.middleware.race.good.GoodQuery;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import com.alibaba.middleware.race.orderSystemImpl.Result;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class OldBuyerIdQuery {
    public static List<Order> findByBuyerId(String buyerId, long starttime, long endtime){
        if (buyerId == null || buyerId.isEmpty()) return null;
        System.out.println("==========:"+buyerId);
        List<Order> orders = new ArrayList<Order>();
        
        TreeMap<Long, Order> rankMap = new TreeMap<Long, Order>();
        
        OrderIndex orderIndex = OrderIndex.getOrderIndexbyBuyerID(buyerId);
		
		Map<Long, Order> page;
		
		if(!PageCache.orderPageMap.containsKey(orderIndex)){
			PageCache.cacheOrderByBuyerID(buyerId);
		}
		
		page = PageCache.orderPageMap.get(orderIndex);
		for(Map.Entry<Long, Order> entry : page.entrySet()){
			Order order = entry.getValue();
			Map<String, KeyValue> keyValues = order.getKeyValues();
			KeyValue keyValue = keyValues.get("createtime");
			long createTime = Long.valueOf(keyValue.getValue());
			keyValue = keyValues.get("buyerid");
			String id = keyValue.getValue();
			
			if(createTime >= starttime && createTime < endtime && buyerId.equals(id)){
				rankMap.put(createTime, order);
			}
		}
		
		for(Entry<Long, Order> entry : rankMap.descendingMap().entrySet()){
			orders.add(entry.getValue());
		}
        return orders;
    }

    public static Iterator<Result> findOrdersByBuyer(long startTime, long endTime, String buyerid) {
        //System.out.println("===queryOrdersByBuyer=====buyerid:" + buyerid + "======starttime:" + startTime + "=========endtime:" + endTime);
        long starttime = System.currentTimeMillis();
        List<Result> results = new ArrayList<Result>();

        int buyerHashIndex = (int) (Math.abs(buyerid.hashCode()) % FileConstant.FILE_BUYER_NUMS);
        Buyer buyer = BuyerQuery.findBuyerById(buyerid, buyerHashIndex);
        if (buyer == null) return results.iterator();

        //获取goodid的所有订单信息
        List<Order> orders = OldBuyerIdQuery.findByBuyerId(buyerid, startTime, endTime);
        if (orders == null || orders.size() == 0) return results.iterator();

        for (Order order : orders) {
            //System.out.println("queryOrdersByBuyer buyerid:"+ buyerid +" : " + order_old.toString());
            Result result = new Result();
            //加入对应买家的所有属性kv
            if (buyer != null && buyer.getKeyValues() != null) {
                result.getKeyValues().putAll(buyer.getKeyValues());
            }
            //加入对应商品的所有属性kv
            int goodIdHashIndex = (int) (Math.abs(order.getKeyValues().get("goodid").getValue().hashCode()) % FileConstant.FILE_GOOD_NUMS);
            Good good = GoodQuery.findGoodById(order.getKeyValues().get("goodid").getValue(), goodIdHashIndex);

            if (good != null && good.getKeyValues() != null) {
                result.getKeyValues().putAll(good.getKeyValues());
            }
            //加入订单信息的所有属性kv
            result.getKeyValues().putAll(order.getKeyValues());
            result.setOrderid(order.getId());
            results.add(result);
        }
        
        System.out.println("queryOrdersByBuyer :" + buyerid + " time :" + (System.currentTimeMillis() - starttime));
        return results.iterator();
    }

    public static void main(String args[]) {

        //BuyerIdIndexFile.generateBuyerIdIndex();
        //findByBuyerId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
