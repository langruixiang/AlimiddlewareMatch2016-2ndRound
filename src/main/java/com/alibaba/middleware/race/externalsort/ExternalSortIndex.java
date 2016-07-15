package com.alibaba.middleware.race.externalsort;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ExternalSortIndex {
	public static class TimeKey implements Comparable<TimeKey>{
		private long beginTime;
		private long endTime;
		
		public TimeKey(long beginTime, long endTime){
			this.beginTime = beginTime;
			this.endTime = endTime;
		}

		@Override
		public int compareTo(TimeKey o) {
			// TODO Auto-generated method stub
			if(beginTime - o.beginTime > 0L){
				return 1;
			}else if(beginTime - o.beginTime < 0L){
				return -1;
			}
			return 0;
		}
		
		@Override
		public String toString(){
			return "BeginTime:" + beginTime + " "
				 + "EndTime" + "EndTime:" + endTime;
		}
	}
	
	private TreeMap<TimeKey, Integer> index = new TreeMap<>();
	
	public List<Integer> findPageNumber(long beginTime, long endTime){
		List<Integer> ret = new ArrayList<Integer>();
		for(Map.Entry<TimeKey, Integer> entry : index.entrySet()){
			if(entry.getKey().beginTime >= beginTime && entry.getKey().beginTime <= endTime){
				ret.add(entry.getValue());
			}else if(entry.getKey().beginTime > endTime){
				break;
			}
		}
		
		return ret;
	}
	
	public void addIndex(TimeKey timeKey, Integer pageNumber){
		index.put(timeKey, pageNumber);
	}
	

	public void print(){
		for(Map.Entry<TimeKey, Integer> entry : index.entrySet()){
			System.out.println(entry.getKey().toString() + "__" + entry.getValue());
		}
	}

}
