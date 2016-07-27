package com.alibaba.middleware.race.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import com.alibaba.middleware.race.constant.FileConstant;

public class OrderIndex {

	public int folderIndex;
	public int hashIndex;
	
	public OrderIndex(int folderIndex, int hashIndex){
		this.folderIndex = folderIndex;
		this.hashIndex = hashIndex;
	}
	
	public static OrderIndex getOrderIndexbyOrderID(long orderid){
		int folderIndex = (int)((orderid / FileConstant.FILENUM_PER_FOLDER) % FileConstant.FOLDER_ORDER_NUMS);
        int hashIndex = (int) (orderid % FileConstant.FILENUM_PER_FOLDER);
        
        return new OrderIndex(folderIndex, hashIndex);
	}
	
	public static OrderIndex getOrderIndexbyGoodID(String goodid){
		int id = Math.abs(goodid.hashCode());
		
		int folderIndex = (int)((id / FileConstant.FILENUM_PER_FOLDER) % FileConstant.FOLDER_ORDER_NUMS);        
        int hashIndex = (int) ( id % FileConstant.FILENUM_PER_FOLDER);
        
        return new OrderIndex(folderIndex, hashIndex);
	}
	
	public static OrderIndex getOrderIndexbyBuyerID(String buyerid){
		 int id = Math.abs(buyerid.hashCode());         
         int folderIndex = (int)((id / FileConstant.FILENUM_PER_FOLDER) % FileConstant.FOLDER_ORDER_NUMS);
         
         int hashIndex = (int) (id %  FileConstant.FILENUM_PER_FOLDER);
         
         return new OrderIndex(folderIndex, hashIndex);
	}
	
	public static BufferedReader getBufferedReaderByOrderID(long orderid) throws FileNotFoundException{
		int folderIndex = (int)((orderid / FileConstant.FILENUM_PER_FOLDER) % FileConstant.FOLDER_ORDER_NUMS);
        int hashIndex = (int) (orderid % FileConstant.FILENUM_PER_FOLDER);
        
        return new BufferedReader(new FileReader(new File(FileConstant.FIRST_DISK_PATH + FileConstant.ORDER_ID_FOLDERNAME + folderIndex
            				+ "/" + FileConstant.FILE_INDEX_BY_ORDERID + hashIndex)));
	}
	
	public static BufferedReader getBufferedReaderByBuyerID(String buyerid) throws FileNotFoundException{
		int id = Math.abs(buyerid.hashCode());         
        int folderIndex = (int)((id / FileConstant.FILENUM_PER_FOLDER) % FileConstant.FOLDER_ORDER_NUMS);
        
        int hashIndex = (int) (id %  FileConstant.FILENUM_PER_FOLDER);
        
        return new BufferedReader(new FileReader(new File(FileConstant.SECOND_DISK_PATH + FileConstant.BUYER_ID_FOLDERNAME + folderIndex
				+ "/" + FileConstant.FILE_INDEX_BY_BUYERID + hashIndex)));
	}
	
	public static BufferedReader getBufferedReaderByGoodID(String goodid) throws FileNotFoundException{
        int id = Math.abs(goodid.hashCode());
		
		int folderIndex = (int)((id / FileConstant.FILENUM_PER_FOLDER) % FileConstant.FOLDER_ORDER_NUMS);        
        int hashIndex = (int) ( id % FileConstant.FILENUM_PER_FOLDER);
        
        return new BufferedReader(new FileReader(new File(FileConstant.THIRD_DISK_PATH + FileConstant.GOOD_ID_FOLDERNAME + folderIndex
            				+ "/" + FileConstant.FILE_INDEX_BY_GOODID + hashIndex)));
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + folderIndex;
		result = prime * result + hashIndex;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OrderIndex other = (OrderIndex) obj;
		if (folderIndex != other.folderIndex)
			return false;
		if (hashIndex != other.hashIndex)
			return false;
		return true;
	}

}
