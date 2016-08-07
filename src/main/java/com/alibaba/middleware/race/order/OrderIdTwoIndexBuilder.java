package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 1. 将所有未排序的orderid一级索引文件排序并存储
 * 2. 根据orderid一级索引生成orderid二级索引并缓存
 * 
 * @author jiangchao
 */
public class OrderIdTwoIndexBuilder extends Thread {

    private CountDownLatch orderIdOneIndexBuilderLatch;

    private CountDownLatch buildIndexCountLatch;

    private int maxConcurrentNum;

    public OrderIdTwoIndexBuilder(CountDownLatch orderIdOneIndexBuilderLatch,
            CountDownLatch buildIndexCountLatch, int maxConcurrentNum) {
        this.orderIdOneIndexBuilderLatch = orderIdOneIndexBuilderLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.maxConcurrentNum = maxConcurrentNum;
    }

    public void build() {
        for (int i = 0; i < Config.ORDER_ONE_INDEX_FILE_NUMBER; i += maxConcurrentNum) {
            int concurrentNum = maxConcurrentNum > (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) ? (Config.ORDER_ONE_INDEX_FILE_NUMBER - i)
                    : maxConcurrentNum;
            CountDownLatch countDownLatch = new CountDownLatch(concurrentNum);
            for (int j = i; j < i + concurrentNum; j++) {
                new MultiIndex(j, countDownLatch, buildIndexCountLatch).start();
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        if (orderIdOneIndexBuilderLatch != null) {
            try {
                orderIdOneIndexBuilderLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long orderIdTwoIndexBuilderStartTime = System.currentTimeMillis();
        build();
        System.out.println("OrderIdTwoIndexBuilder work end! time : "
                + (System.currentTimeMillis() - orderIdTwoIndexBuilderStartTime));
    }

    /**
     * 每个MultiIndex 负责一个一级索引文件，完成的任务有:
     * 1. 将orderid一级索引文件排序并存储
     * 2. 根据orderid一级索引生成orderid二级索引并缓存
     * 
     */
    private class MultiIndex extends Thread {
        private int index;
        private CountDownLatch selfCountDownLatch;
        private CountDownLatch parentCountDownLatch;

        public MultiIndex(int index, CountDownLatch selfCountDownLatch,
                CountDownLatch parentCountDownLatch) {
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
                order_records = new FileInputStream(Config.FIRST_DISK_PATH
                        + FileConstant.UNSORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX
                        + index);

                BufferedReader order_br = new BufferedReader(
                        new InputStreamReader(order_records));

                File file = new File(Config.FIRST_DISK_PATH
                        + FileConstant.SORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX
                        + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);
                String str = null;
                long count = 0;
                String orderid = null;
                while ((str = order_br.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(str,
                            ":");
                    while (stringTokenizer.hasMoreElements()) {
                        orderIndex.put(
                                Long.valueOf(stringTokenizer.nextToken()), str);
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

    // public static long bytes2Long(byte[] byteNum) {
    // long num = 0;
    // for (int ix = 0; ix < 8; ++ix) {
    // num <<= 8;
    // num |= (byteNum[ix] & 0xff);
    // }
    // return num;
    // }

}
