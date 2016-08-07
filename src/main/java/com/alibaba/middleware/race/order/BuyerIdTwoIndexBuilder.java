package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.IndexSizeCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 1. 将所有未排序的buyerid一级索引文件排序并存储 2. 根据buyerid一级索引生成buyerid二级索引并缓存
 * 
 * 存放位置：第二个硬盘
 * 
 * @author jiangchao
 */
public class BuyerIdTwoIndexBuilder extends Thread {

    private CountDownLatch buyerIdOneIndexBuilderLatch;

    private CountDownLatch buildIndexCountLatch;

    private int maxConcurrentNum;

    public BuyerIdTwoIndexBuilder(CountDownLatch buyerIdOneIndexBuilderLatch,
            CountDownLatch buildIndexCountLatch, int maxConcurrentNum) {
        this.buyerIdOneIndexBuilderLatch = buyerIdOneIndexBuilderLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.maxConcurrentNum = maxConcurrentNum;
    }

    public void build() {
        for (int i = 0; i < Config.ORDER_ONE_INDEX_FILE_NUMBER; i += maxConcurrentNum) {
            int concurrentNum = maxConcurrentNum > (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) ? (Config.ORDER_ONE_INDEX_FILE_NUMBER - i)
                    : maxConcurrentNum;
            CountDownLatch multiIndexLatch = new CountDownLatch(concurrentNum);
            for (int j = i; j < i + concurrentNum; j++) {
                new BuyerIdTwoIndexBuilder.MultiIndex(j, multiIndexLatch,
                        buildIndexCountLatch).start();
            }
            try {
                multiIndexLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        if (buyerIdOneIndexBuilderLatch != null) {
            try {
                buyerIdOneIndexBuilderLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long startTime = System.currentTimeMillis();
        build();
        System.out
                .printf("BuyerIdTwoIndexBuilder work end! Used time：%d End time : %d %n",
                        System.currentTimeMillis() - startTime,
                        System.currentTimeMillis()
                                - OrderSystemImpl.constructStartTime);
    }

    /**
     * 每个MultiIndex 负责一个一级索引文件，完成的任务有: 1. 将buyerid一级索引文件排序并存储 2.
     * 根据buyerid一级索引生成buyerid二级索引并缓存
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
            TreeMap<String, TreeMap<Long, String>> buyerIndex = new TreeMap<String, TreeMap<Long, String>>();
            TreeMap<String, Long> twoIndexMap = new TreeMap<String, Long>();
            try {
                BufferedReader unSortedOneIndexBr = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(
                                        Config.SECOND_DISK_PATH
                                                + FileConstant.UNSORTED_BUYER_ID_ONE_INDEX_FILE_PREFIX
                                                + index)));

                BufferedWriter sortedOneIndexBw = new BufferedWriter(
                        new FileWriter(
                                Config.SECOND_DISK_PATH
                                        + FileConstant.SORTED_BUYER_ID_ONE_INDEX_FILE_PREFIX
                                        + index));

                String line = null;
                while ((line = unSortedOneIndexBr.readLine()) != null) {
                    String buyerid = null;
                    String createtime = null;
                    String fileName = null;
                    String position = null;
                    String content = null;
                    StringTokenizer stringTokenizer = new StringTokenizer(line,
                            ":");
                    while (stringTokenizer.hasMoreElements()) {
                        buyerid = stringTokenizer.nextToken();
                        createtime = stringTokenizer.nextToken();
                        fileName = stringTokenizer.nextToken();
                        position = stringTokenizer.nextToken();
                        content = fileName + "_" + position;
                        if (!buyerIndex.containsKey(buyerid)) {
                            buyerIndex
                                    .put(buyerid, new TreeMap<Long, String>());
                        }
                        buyerIndex.get(buyerid).put(Long.valueOf(createtime),
                                content);
                        break;
                    }
                }

                int twoIndexSize = (int) Math.sqrt(buyerIndex.size());
                IndexSizeCache.buyerIdIndexRegionSizeMap.put(index,
                        twoIndexSize);
                long count = 0;
                long position = 0;
                Iterator<Map.Entry<String, TreeMap<Long, String>>> iterator = buyerIndex
                        .entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry<String, TreeMap<Long, String>> entry = iterator
                            .next();
                    String key = (String) entry.getKey();
                    TreeMap<Long, String> val = (TreeMap<Long, String>) entry
                            .getValue();

                    StringBuilder content = new StringBuilder(key + "\t");
                    Iterator<Map.Entry<Long, String>> iteratorOrders = val
                            .descendingMap().entrySet().iterator();
                    while (iteratorOrders.hasNext()) {
                        Map.Entry<Long, String> orderEntry = iteratorOrders
                                .next();
                        Long createtime = (Long) orderEntry.getKey();
                        String pos = (String) orderEntry.getValue();
                        content.append(createtime);
                        content.append(":");
                        content.append(pos);
                        content.append("|");
                    }
                    val.clear();
                    sortedOneIndexBw.write(content.toString() + '\n');

                    if (count % twoIndexSize == 0) {
                        twoIndexMap.put(key, position);
                    }
                    position += content.toString().getBytes().length + 1;
                    count++;
                }
                TwoIndexCache.buyerIdTwoIndexCache.put(index, twoIndexMap);
                buyerIndex.clear();
                sortedOneIndexBw.flush();
                sortedOneIndexBw.close();
                unSortedOneIndexBr.close();
                selfCountDownLatch.countDown();
                parentCountDownLatch.countDown();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
