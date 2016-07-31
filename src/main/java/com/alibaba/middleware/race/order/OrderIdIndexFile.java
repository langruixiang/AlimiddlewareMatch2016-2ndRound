package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class OrderIdIndexFile extends Thread{

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int concurrentNum;

    private long orderIdHashTime;

    public OrderIdIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int concurrentNum, long orderIdHashTime) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.concurrentNum = concurrentNum;
        this.orderIdHashTime = orderIdHashTime;
    }

    //订单文件按照orderid生成索引文件，存放到第三块磁盘上
    public void generateOrderIdIndex() {

        for (int i = 0; i < FileConstant.FILE_ORDER_NUMS; i+=concurrentNum) {
            int num = concurrentNum > (FileConstant.FILE_ORDER_NUMS - i) ? (FileConstant.FILE_ORDER_NUMS - i) : concurrentNum;
            CountDownLatch countDownLatch = new CountDownLatch(num);
            for (int j = i; j < i+num; j++) {
                new MultiIndex(j, countDownLatch, buildIndexCountLatch).start();
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void run(){
        if (hashDownLatch != null) {
            try {
                hashDownLatch.await();//等待上一个任务的完成
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("orderid hash order end, time:" + (System.currentTimeMillis() - orderIdHashTime));
        long indexStartTime = System.currentTimeMillis();
        generateOrderIdIndex();
        System.out.println("orderid build index " + " work end! time : " + (System.currentTimeMillis() - indexStartTime));
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
            Map<Long, String> orderIndex = new TreeMap<Long, String>();
            TreeMap<Long, Long> twoIndexMap = new TreeMap<Long, Long>();
            FileInputStream order_records = null;
            try {
                order_records = new FileInputStream(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_INDEX_BY_ORDERID + index);

                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File file = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_ORDERID + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);
                String str = null;
                long count = 0;
                String orderid = null;
                while ((str = order_br.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(str, ":");
                    while (stringTokenizer.hasMoreElements()) {
                        orderIndex.put(Long.valueOf(stringTokenizer.nextToken()), str);
                        break;
                    }
                }

                int towIndexSize = (int) Math.sqrt(orderIndex.size());
                FileConstant.orderIdIndexRegionSizeMap.put(index, towIndexSize);
                count = 0;
                long position = 0;
                Iterator iterator = orderIndex.entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry entry = (Map.Entry) iterator.next();
                    Long key = (Long) entry.getKey();
                    String val = (String) entry.getValue();
                    bufferedWriter.write(val + '\n');

                    if (count % towIndexSize == 0) {
                        twoIndexMap.put(key, position);
                    }
                    position += val.getBytes().length + 1;
                    count++;
                }
                TwoIndexCache.orderIdTwoIndexCache.put(index, twoIndexMap);
                orderIndex.clear();
                bufferedWriter.flush();
                bufferedWriter.close();
                order_br.close();
                selfCountDownLatch.countDown();
                parentCountDownLatch.countDown();
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
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
