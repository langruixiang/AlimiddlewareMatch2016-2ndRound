package com.alibaba.middleware.race.unused;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.good.GoodIdIndexFile;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class BuyerIdIndexFile extends Thread{

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int concurrentNum;

    public BuyerIdIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int concurrentNum) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.concurrentNum = concurrentNum;
    }

    //订单文件按照buyerid生成索引文件，存放到第二块磁盘上
    public void generateBuyerIdIndex() {
        for (int i = 0; i < Config.ORDER_ONE_INDEX_FILE_NUMBER; i+=concurrentNum) {
            int num = concurrentNum > (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) ? (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) : concurrentNum;
            CountDownLatch countDownLatch = new CountDownLatch(num);
            for (int j = i; j < i+num; j++) {
                new BuyerIdIndexFile.MultiIndex(j, countDownLatch, buildIndexCountLatch).start();
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
                hashDownLatch.await(); //等待上一个任务的结束
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        generateBuyerIdIndex();
        System.out.println("buyerid build index " + " work end!");
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
            Map<String, String> orderRankMap = new TreeMap<String, String>().descendingMap();
            Map<String, Long> buyerIndex = new LinkedHashMap<String, Long>();
            TreeMap<String, Long> twoIndexMap = new TreeMap<String, Long>();
            try {
                FileInputStream order_records = new FileInputStream(Config.SECOND_DISK_PATH + FileConstant.UNSORTED_BUYER_ID_ONE_INDEX_FILE_PREFIX + index);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File fileRank = new File(Config.SECOND_DISK_PATH + FileConstant.FILE_RANK_BY_BUYERID + index);
                FileWriter fwRank = new FileWriter(fileRank);
                BufferedWriter rankBW = new BufferedWriter(fwRank);

                File file = new File(Config.SECOND_DISK_PATH + FileConstant.SORTED_BUYER_ID_ONE_INDEX_FILE_PREFIX + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);


                String rankStr = null;
                while ((rankStr = order_br.readLine()) != null) {
                    String buyerid = null;
                    String createtime = null;
                    String[] keyValues = rankStr.split("\t");
                    for (int j = 0; j < keyValues.length; j++) {
                        String[] keyValue = keyValues[j].split(":");

                        if ("buyerid".equals(keyValue[0])) {
                            buyerid = keyValue[1];
                        } else if ("createtime".equals(keyValue[0])) {
                            createtime = keyValue[1];
                        }
                        if (buyerid != null && createtime != null) {
                            String newKey = buyerid + "_" + createtime;
                            orderRankMap.put(newKey, rankStr);
                            break;
                        }
                    }
                }

                long position = 0;
                Iterator orderRankIterator = orderRankMap.entrySet().iterator();
                while (orderRankIterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) orderRankIterator.next();
                    String key = (String) entry.getKey();
                    String val = (String) entry.getValue();
                    rankBW.write(val + '\n');

                    if (!buyerIndex.containsKey(key)) {
                        buyerIndex.put(key, position);
                    }
                    position += val.getBytes().length + 1;
                }

                int twoIndexSize = (int) Math.sqrt(buyerIndex.size());
                FileConstant.buyerIdIndexRegionSizeMap.put(index, twoIndexSize);
                int count = 0;
                long oneIndexPosition = 0;
                Iterator iterator = buyerIndex.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    String key = (String) entry.getKey();
                    Long val = (Long) entry.getValue();
                    String content = key + ":" + val;
                    bufferedWriter.write(content + '\n');

                    if (count%twoIndexSize == 0) {
                        twoIndexMap.put(key, oneIndexPosition);
                    }
                    oneIndexPosition += content.getBytes().length + 1;
                    count++;
                }
                TwoIndexCache.buyerIdTwoIndexCache.put(index, twoIndexMap);
                buyerIndex.clear();
                rankBW.flush();
                rankBW.close();
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
}
