package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class OrderHashFile extends Thread{

    private Collection<String> orderFiles;
    private Collection<String> storeFolders;
    private int nums;
    private String type;
    private CountDownLatch countDownLatch;

    public OrderHashFile(Collection<String> orderFiles,  Collection<String> storeFolders, int nums, String type,
                         CountDownLatch countDownLatch) {
        this.orderFiles = orderFiles;
        this.storeFolders = storeFolders;
        this.nums = nums;
        this.type = type;
        this.countDownLatch = countDownLatch;
    }

    //读取所有订单文件，按照订单号hash到多个小文件中
    public void generateOrderIdHashFile() {

        try {
            BufferedWriter[] bufferedWriters = new BufferedWriter[nums];

            for (int i = 0; i < nums; i++) {
                File file = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_INDEX_BY_ORDERID + i);
                FileWriter fw = null;
                fw = new FileWriter(file);
                bufferedWriters[i] = new BufferedWriter(fw);
            }

            CountDownLatch multiHashLatch = new CountDownLatch(orderFiles.size());
            for (String orderFile : orderFiles) {            	
            	new MultiHash(orderFile, multiHashLatch, "orderid", bufferedWriters).start();
            }            
            multiHashLatch.await();

            for (int i = 0; i < nums; i++) {
                bufferedWriters[i].close();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    //读取所有订单文件，按照订单中的买家ID hash到多个小文件中
    public void generateBuyerIdHashFile() {
        try {
            BufferedWriter[] bufferedWriters = new BufferedWriter[nums];

            for (int i = 0; i < nums; i++) {
                File file = new File(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_INDEX_BY_BUYERID + i);
                FileWriter fw = new FileWriter(file);
                bufferedWriters[i] = new BufferedWriter(fw);
            }

            CountDownLatch multiHashLatch = new CountDownLatch(orderFiles.size());
            for (String orderFile : orderFiles) {            	
            	new MultiHash(orderFile, multiHashLatch, "buyerid", bufferedWriters).start();
            }            
            multiHashLatch.await();

            for (int i = 0; i < nums; i++) {
                bufferedWriters[i].close();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    //读取所有订单文件，按照订单中的商品ID hash到多个小文件中
    public void generateGoodIdHashFile() {
        try {
            BufferedWriter[] bufferedWriters = new BufferedWriter[nums];

            for (int i = 0; i < nums; i++) {
                File file = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + i);
                FileWriter fw = new FileWriter(file);
                bufferedWriters[i] = new BufferedWriter(fw);
            }

            CountDownLatch multiHashLatch = new CountDownLatch(orderFiles.size());
            for (String orderFile : orderFiles) {            	
            	new MultiHash(orderFile, multiHashLatch, "goodid", bufferedWriters).start();
            }            
            multiHashLatch.await();

            for (int i = 0; i < nums; i++) {
                bufferedWriters[i].close();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        if (type.equals("orderid")) {
            generateOrderIdHashFile();
        } else if (type.equals("goodid")) {
            generateGoodIdHashFile();
        } else {
            generateBuyerIdHashFile();
        }
        countDownLatch.countDown();//完成工作，计数器减一
    }

    public static void main(String args[]) {
        List<String> orderFileList = new ArrayList<String>();
        orderFileList.add("order_records.txt");

        List<String> buyerFileList = new ArrayList<String>();
        buyerFileList.add("buyer_records.txt");

        List<String> goodFileList = new ArrayList<String>();
        goodFileList.add("good_records.txt");

        //generateBuyerIdHashFile(orderFileList, buyerFileList, goodFileList, orderFileList, 25);
    }
    
    private class MultiHash extends Thread{
    	private String orderFile;
    	private CountDownLatch countDownLatch;
    	private String type;
    	private BufferedWriter[] bufferedWriters;
    	
    	public MultiHash(String orderFile, CountDownLatch countDownLatch, String type, BufferedWriter[] bufferedWriters){
    		this.orderFile = orderFile;
    		this.countDownLatch = countDownLatch;
    		this.type = type;
    		this.bufferedWriters = bufferedWriters;
    	}
    	
    	@Override
    	public void run(){
    		try {
				FileInputStream order_records = new FileInputStream(orderFile);
				BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

				String str = null;
				String buyerid = null;
				String goodid = null;
				Long orderid = null;
				
				int hashFileIndex;
				if(type.equals("orderid")){
                    long position = 0;
					while ((str = order_br.readLine()) != null) {
                        StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                        while (stringTokenizer.hasMoreElements()) {
                            StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                            String key = keyValue.nextToken();
                            String value = keyValue.nextToken();
	                        if ("orderid".equals(key)) {
	                            orderid = Long.valueOf(value);
	                            hashFileIndex = (int) (orderid % nums);
                                String content = orderid + ":" + orderFile + ":" + position + '\n';
	                            synchronized (bufferedWriters[hashFileIndex]) {
									bufferedWriters[hashFileIndex].write(content);
								}
                                position += str.getBytes().length + 1;
                                break;
	                        }
	                    }
	                }
				}else if(type.equals("goodid")){
					while ((str = order_br.readLine()) != null) {
                        StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                        while (stringTokenizer.hasMoreElements()) {
                            StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                            String key = keyValue.nextToken();
                            String value = keyValue.nextToken();

					        if (type.equals(key)) {
					            goodid = value;
					            hashFileIndex = (int) (Math.abs(goodid.hashCode()) % nums);
					            synchronized (bufferedWriters[hashFileIndex]) {
									bufferedWriters[hashFileIndex].write(str + '\n');
								}
                                break;
					        }
					    }
					}
				}else if(type.equals("buyerid")){
	                while ((str = order_br.readLine()) != null) {
                        StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                        while (stringTokenizer.hasMoreElements()) {
                            StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                            String key = keyValue.nextToken();
                            String value = keyValue.nextToken();
	                        if ("buyerid".equals(key)) {
	                            buyerid = value;
	                            hashFileIndex = (int) (Math.abs(buyerid.hashCode()) % nums);
	                            synchronized (bufferedWriters[hashFileIndex]) {
									bufferedWriters[hashFileIndex].write(str + '\n');
								}
								//bufferedWriters[hashFileIndex].newLine();
	                            break;
	                        }
	                    }
	                }
				}
				
				countDownLatch.countDown();
                System.out.println(type + "hash file end :" + orderFile);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
}
