package com.alibaba.middleware.race.unused;

import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class BuyerIndexFile extends Thread{

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int index;

    private long buyerHashTime;

    public BuyerIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int index, long buyerHashTime) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.index = index;
        this.buyerHashTime = buyerHashTime;
    }

    //订单文件按照buyerid生成索引文件，存放到第二块磁盘上
    public void generateBuyerIndex() {

            try {
                FileInputStream buyer_records = new FileInputStream(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_BUYER_HASH + index);
                BufferedReader buyer_br = new BufferedReader(new InputStreamReader(buyer_records));

                String str = null;
                long position = 0;
                while ((str = buyer_br.readLine()) != null) {
                    String buyerid = null;
                    String[] keyValues = str.split("\t");
                    for (int j = 0; j < keyValues.length; j++) {
                        String[] keyValue = keyValues[j].split(":");

                        if ("buyerid".equals(keyValue[0])) {
                            buyerid = keyValue[1];
                            //OneIndexCache.buyerOneIndexCache.put(buyerid, position);
                            break;
                        }
                    }
                    position += str.getBytes().length + 1;
                }
                buyer_br.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    //}

    public void run(){
        if (hashDownLatch != null) {
            try {
                hashDownLatch.await(); //等待上一个任务的结束
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("buyer file hash end, time:" + (System.currentTimeMillis() - buyerHashTime));
        long indexStartTime = System.currentTimeMillis();
        generateBuyerIndex();
        buildIndexCountLatch.countDown();
        System.out.println("buyer build index " + index + " work end! time : " + (System.currentTimeMillis() - indexStartTime));
    }
}
