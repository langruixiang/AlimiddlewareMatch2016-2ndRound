package com.alibaba.middleware.race.buyer;

import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class OldBuyerIdIndexFile extends Thread{

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int concurrentNum;

    private long buyerIdHashTime;

    public OldBuyerIdIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int concurrentNum, long buyerIdHashTime) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.concurrentNum = concurrentNum;
        this.buyerIdHashTime = buyerIdHashTime;
    }

    //订单文件按照buyerid生成索引文件，存放到第二块磁盘上
    public void generateBuyerIdIndex() {
        for (int i = 0; i < FileConstant.FILE_ORDER_NUMS; i+=concurrentNum) {
            int num = concurrentNum > (FileConstant.FILE_ORDER_NUMS - i) ? (FileConstant.FILE_ORDER_NUMS - i) : concurrentNum;
            CountDownLatch countDownLatch = new CountDownLatch(num);
            for (int j = i; j < i+num; j++) {
                new OldBuyerIdIndexFile.MultiIndex(j, countDownLatch, buildIndexCountLatch).start();
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
        System.out.println("buyerid hash order end, time:" + (System.currentTimeMillis() - buyerIdHashTime));
        long indexStartTime = System.currentTimeMillis();
        generateBuyerIdIndex();
        System.out.println("buyerid build index " + " work end! time : " + (System.currentTimeMillis() - indexStartTime));
    }

    private class MultiIndex extends Thread{
        private int index;
        private CountDownLatch selfCountDownLatch;
        private CountDownLatch parentCountDownLatch;

        public MultiIndex(int index, CountDownLatch selfCountDownLatch, CountDownLatch parentCountDownLatch){
            this.index = index;
            this.selfCountDownLatch = selfCountDownLatch;
            this.parentCountDownLatch = parentCountDownLatch;
        }

        @Override
        public void run() {
            TreeMap<String, TreeMap<Long, String>> buyerIndex = new TreeMap<String, TreeMap<Long, String>>();
            TreeMap<String, Long> twoIndexMap = new TreeMap<String, Long>();
            //for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            try {
                FileInputStream order_records = new FileInputStream(
                        FileConstant.SECOND_DISK_PATH + FileConstant.FILE_INDEX_BY_BUYERID + index);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File file = new File(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_BUYERID + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                String str = null;

                while ((str = order_br.readLine()) != null) {
                    String buyerid = null;
                    String createtime = null;
                    String fileName = null;
                    String position = null;
                    String content = null;
                    StringTokenizer stringTokenizer = new StringTokenizer(str, ":");
                    while (stringTokenizer.hasMoreElements()) {
                        buyerid = stringTokenizer.nextToken();
                        createtime = stringTokenizer.nextToken();
                        fileName = stringTokenizer.nextToken();
                        position = stringTokenizer.nextToken();
                        content = fileName + "_" + position;
                        if (!buyerIndex.containsKey(buyerid)) {
                            buyerIndex.put(buyerid, new TreeMap<Long, String>());
                        }
                        buyerIndex.get(buyerid).put(Long.valueOf(createtime), content);
                        break;
                    }
                }

                int twoIndexSize = (int) Math.sqrt(buyerIndex.size());
                FileConstant.buyerIdIndexRegionSizeMap.put(index, twoIndexSize);
                long count = 0;
                long position = 0;
                Iterator iterator = buyerIndex.entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry entry = (Map.Entry) iterator.next();
                    String key = (String) entry.getKey();
                    TreeMap<Long, String> val = (TreeMap<Long, String>) entry.getValue();

                    StringBuilder content = new StringBuilder(key + "\t");
                    Iterator iteratorOrders = val.descendingMap().entrySet().iterator();
                    while (iteratorOrders.hasNext()) {
                        Map.Entry orderEntry = (Map.Entry) iteratorOrders.next();
                        Long createtime = (Long) orderEntry.getKey();
                        String pos = (String)orderEntry.getValue();
                        content.append(createtime);
                        content.append(":");
                        content.append(pos);
                        content.append("|");
                    }
                    val.clear();
                    bufferedWriter.write(content.toString() + '\n');

                    if (count % twoIndexSize == 0) {
                        twoIndexMap.put(key, position);
                    }
                    position += content.toString().getBytes().length + 1;
                    count++;
                }
                TwoIndexCache.buyerIdTwoIndexCache.put(index, twoIndexMap);
                buyerIndex.clear();
                bufferedWriter.flush();
                bufferedWriter.close();
                order_br.close();
                selfCountDownLatch.countDown();
                parentCountDownLatch.countDown();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
