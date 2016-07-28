package com.alibaba.middleware.race.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Order;

public class IDCache {
	public static OrderIDCache orderIDCache = new OrderIDCache(FileConstant.MAX_CONCURRENT, 20);
    
    public static BuyerGoodIDCache buyerIDCache = new BuyerGoodIDCache(FileConstant.MAX_CONCURRENT, 20);
    
    public static BuyerGoodIDCache goodIDCache = new BuyerGoodIDCache(FileConstant.MAX_CONCURRENT, 20);
    
    public static class OrderIDCache{
    	private int size;
    	private int numOfMap;
    	private int sizeOfMap;
    	private List<Map<Long, Order>> cacheList = new ArrayList<Map<Long, Order>>();
    	
    	private AtomicInteger numOfSearch = new AtomicInteger(0);
    	private AtomicInteger numOfHit = new AtomicInteger(0);
    	
    	public OrderIDCache(int size, int numOfMap){
    		this.size = size;
    		this.numOfMap = numOfMap;
    		this.sizeOfMap = size / numOfMap;
    		
    		for(int i = 0; i < numOfMap; i++){
    			Map<Long, Order> map = new LinkedHashMap<Long, Order>(sizeOfMap, 1) {
    		        protected boolean removeEldestEntry(Map.Entry eldest) {
    		            return size() > sizeOfMap;
    		        }
    		    };
    		    
    		    cacheList.add(map);
    		}
    	}
    	
    	public Order getOrderByOrderID(long orderID){
    		numOfSearch.incrementAndGet();
    		
    		int indexOfMap = (int)(orderID % numOfMap);
    		Map<Long, Order> map = cacheList.get(indexOfMap);
    		
    		Order order = null;
    		synchronized (map) {
				order = map.get(orderID);
			}
    		
    		if(order != null){
    			numOfHit.incrementAndGet();
    		}
    		
    		System.out.println("OrderIDCache Hit rate:" + (numOfHit.get() * 1.0 / numOfSearch.get()));
    		
    		return order;
    	}
    	
    	public void putOrderByOrderID(long orderID, Order order){
    		int indexOfMap = (int)(orderID % numOfMap);
    		Map<Long, Order> map = cacheList.get(indexOfMap);
    		
    		synchronized (map) {
				map.put(orderID, order);
			}
    	}
    	
    }
    
    public static class BuyerGoodIDCache{
    	private int size;
    	private int numOfMap;
    	private int sizeOfMap;
    	private List<Map<String, List<Order>>> cacheList = new ArrayList<Map<String, List<Order>>>();
    	
    	private AtomicInteger numOfSearch = new AtomicInteger(0);
    	private AtomicInteger numOfHit = new AtomicInteger(0);
    	
    	public BuyerGoodIDCache(int size, int numOfMap){
    		this.size = size;
    		this.numOfMap = numOfMap;
    		this.sizeOfMap = size / numOfMap;
    		
    		for(int i = 0; i < numOfMap; i++){
    			Map<String, List<Order>> map = new LinkedHashMap<String, List<Order>>(sizeOfMap, 1) {
    		        protected boolean removeEldestEntry(Map.Entry eldest) {
    		            return size() > sizeOfMap;
    		        }
    		    };
    		    
    		    cacheList.add(map);
    		}
    	}
    	
    	public List<Order> getOrderListByID(String id){
    		numOfSearch.incrementAndGet();
    		
    		int indexOfMap = (int) (Math.abs(id.hashCode()) % numOfMap);
    		Map<String, List<Order>> map = cacheList.get(indexOfMap);
    		
    		List<Order> ret = null;
    		synchronized (map) {
				ret = map.get(id);
			}
    		
    		if(ret != null){
    			numOfHit.incrementAndGet();
    		}
    		
    		System.out.println("BuyerGoodIDCache Hit rate:" + (numOfHit.get() * 1.0/ numOfSearch.get()));
    		
    		return ret;
    	}
    	
    	public void putOrderLisrByID(String id, List<Order> list){
    		int indexOfMap = (int) (Math.abs(id.hashCode()) % numOfMap);
    		Map<String, List<Order>> map = cacheList.get(indexOfMap);
    		
    		synchronized (map) {
    			map.put(id, list);				
			}
    	}
    }

}
