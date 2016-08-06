package com.alibaba.middleware.race.unused;

import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class OldGoodIdIndexFile extends Thread{

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int concurrentNum;

    private long goodIdHashTime;

    public OldGoodIdIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int concurrentNum, long goodIdHashTime) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.concurrentNum = concurrentNum;
        this.goodIdHashTime = goodIdHashTime;
    }

    //订单文件按照goodid生成索引文件，存放到第三块磁盘上
    public void generateGoodIdIndex() {
        for (int i = 0; i < FileConstant.FILE_ORDER_NUMS; i+=concurrentNum) {
            int num = concurrentNum > (FileConstant.FILE_ORDER_NUMS - i) ? (FileConstant.FILE_ORDER_NUMS - i) : concurrentNum;
            CountDownLatch countDownLatch = new CountDownLatch(num);
            for (int j = i; j < i + num; j++) {
                new OldGoodIdIndexFile.MultiIndex(j, countDownLatch, buildIndexCountLatch).start();
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
        System.out.println("goodid hash order end, time:" + (System.currentTimeMillis() - goodIdHashTime));
        long indexStartTime = System.currentTimeMillis();
        generateGoodIdIndex();
        System.out.println("goodid build index " + " work end! time: " + (System.currentTimeMillis() - indexStartTime));
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
            Map<String, TreeMap<Long, String>> goodIndex = new TreeMap<String, TreeMap<Long, String>>();
            TreeMap<String, Long> twoIndexMap = new TreeMap<String, Long>();
            try {
                FileInputStream order_records = new FileInputStream(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + index);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File file = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_GOODID + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                String str = null;
                while ((str = order_br.readLine()) != null) {
                    String orderid = null;
                    String goodid = null;
                    String fileName = null;
                    String position = null;
                    String content = null;
                    StringTokenizer stringTokenizer = new StringTokenizer(str, ":");
                    while (stringTokenizer.hasMoreElements()) {
                        goodid = stringTokenizer.nextToken();
                        orderid = stringTokenizer.nextToken();
                        fileName = stringTokenizer.nextToken();
                        position = stringTokenizer.nextToken();
                        content = fileName + "_" + position;

                        if (!goodIndex.containsKey(goodid)) {
                            goodIndex.put(goodid, new TreeMap<Long, String>());
                        }
                        goodIndex.get(goodid).put(Long.valueOf(orderid), content);
                        break;
                    }
                }

                int towIndexSize = (int) Math.sqrt(goodIndex.size());
                FileConstant.goodIdIndexRegionSizeMap.put(index, towIndexSize);
                long count = 0;
                long position = 0;
                Iterator iterator = goodIndex.entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry entry = (Map.Entry) iterator.next();
                    String key = (String) entry.getKey();
                    Map<String, String> val = (Map<String, String>) entry.getValue();
                    StringBuilder content = new StringBuilder(key + ":");
                    Iterator iteratorOrders = val.entrySet().iterator();
                    while (iteratorOrders.hasNext()) {
                        Map.Entry orderEntry = (Map.Entry) iteratorOrders.next();
                        String pos = (String)orderEntry.getValue();
                        content.append(pos);
                        content.append("|");
                    }
                    val.clear();
                    bufferedWriter.write(content.toString() + '\n');

                    if (count%towIndexSize == 0) {
                        twoIndexMap.put(key, position);
                    }
                    position += content.toString().getBytes().length + 1;
                    count++;
                }
                TwoIndexCache.goodIdTwoIndexCache.put(index, twoIndexMap);
                goodIndex.clear();
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
