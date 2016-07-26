package com.alibaba.middleware.race.buyer;

import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.PosInfo;

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

    public BuyerIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int index) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.index = index;
    }

    //订单文件按照buyerid生成索引文件，存放到第二块磁盘上
    public void generateBuyerIndex() {

            try {
                FileInputStream buyer_records = new FileInputStream(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_BUYER_HASH + index);
                BufferedReader buyer_br = new BufferedReader(new InputStreamReader(buyer_records));

                String line = null;
                int offset = 0;
                int length = 0;
                while ((line = buyer_br.readLine()) != null) {
                    int buyerIdStartIndex = line.indexOf("buyerid:") + 8;// 7 : "buyerid:".length()
                    int buyerIdEndIndex = line.indexOf('\t', buyerIdStartIndex);
                    if (buyerIdEndIndex < 0) {
                        buyerIdEndIndex = line.indexOf('\n', buyerIdStartIndex);
                    }
                    String buyerId = line.substring(buyerIdStartIndex, buyerIdEndIndex);
                    length = line.getBytes().length + 1;
                    OneIndexCache.buyerOneIndexCache.put(buyerId, new PosInfo(offset, length));
                    offset += length;
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
        generateBuyerIndex();
        buildIndexCountLatch.countDown();
        System.out.println("buyer build index " + index + " work end!");
    }


//    public static long bytes2Long(byte[] byteNum) {
//        long num = 0;
//        for (int ix = 0; ix < 8; ++ix) {
//            num <<= 8;
//            num |= (byteNum[ix] & 0xff);
//        }
//        return num;
//    }

}
