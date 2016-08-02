package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.order.OrderIdIndexFile;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class GoodIdIndexFile extends Thread{

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int concurrentNum;

    private long goodIdHashTime;

    public GoodIdIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int concurrentNum, long goodIdHashTime) {
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
                new GoodIdIndexFile.MultiIndex(j, countDownLatch, buildIndexCountLatch).start();
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
            Map<String, TreeMap<Long, String>> orderRankMap = new TreeMap<String, TreeMap<Long, String>>();
            Map<String, String> goodIndex = new LinkedHashMap<String, String>();
            TreeMap<String, Long> twoIndexMap = new TreeMap<String, Long>();
            //for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            try {
                FileInputStream order_records = new FileInputStream(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + index);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File fileRank = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_RANK_BY_GOODID + index);
                FileWriter fwRank = new FileWriter(fileRank);
                BufferedWriter rankBW = new BufferedWriter(fwRank);

                File file = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_GOODID + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                String rankStr = null;
                while ((rankStr = order_br.readLine()) != null) {
                    String orderid = null;
                    String goodid = null;

                    StringTokenizer stringTokenizer = new StringTokenizer(rankStr, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        //String[] keyValue = stringTokenizer.nextToken().split(":");
                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        if ("orderid".equals(key)) {
                            orderid = value;
                        } else if ("goodid".equals(key)) {
                            goodid = value;
                            if (!orderRankMap.containsKey(goodid)) {
                                orderRankMap.put(goodid, new TreeMap<Long, String>());
                            }
                        }
                        if (orderid != null && goodid != null) {
                            orderRankMap.get(goodid).put(Long.valueOf(orderid), rankStr);
                            break;
                        }
                    }
                }

                long position = 0;
                Iterator orderRankIterator = orderRankMap.entrySet().iterator();
                while (orderRankIterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) orderRankIterator.next();
                    String key = (String) entry.getKey();
                    Map<Long, String> val = (Map<Long, String>) entry.getValue();
                    int length = 0;
                    String goodid = key;

                    Iterator orderIdIterator = val.entrySet().iterator();
                    while (orderIdIterator.hasNext()) {
                        Map.Entry orderKv = (Map.Entry) orderIdIterator.next();
                        String orderKvValue = (String) orderKv.getValue();
                        rankBW.write(orderKvValue + '\n');
                        length += orderKvValue.getBytes().length + 1;
                    }
                    if (!goodIndex.containsKey(goodid)) {
                        String posInfo = position + ":" + length + ":" + val.size();
                        goodIndex.put(goodid, posInfo);
                    }
                    position += length;
                    val.clear();
                }

                int towIndexSize = (int) Math.sqrt(goodIndex.size());
                FileConstant.goodIdIndexRegionSizeMap.put(index, towIndexSize);
                int count = 0;
                long oneIndexPosition = 0;
                Iterator iterator = goodIndex.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    String key = (String) entry.getKey();
                    String val = (String) entry.getValue();
                    String content = key + ":" + val;
                    bufferedWriter.write(content + '\n');

                    if (count%towIndexSize == 0) {
                        twoIndexMap.put(key, oneIndexPosition);
                    }
                    oneIndexPosition += content.getBytes().length + 1;
                    count++;
                }
                TwoIndexCache.goodIdTwoIndexCache.put(index, twoIndexMap);
                orderRankMap.clear();
                goodIndex.clear();
                bufferedWriter.flush();
                bufferedWriter.close();
                rankBW.flush();
                rankBW.close();
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
