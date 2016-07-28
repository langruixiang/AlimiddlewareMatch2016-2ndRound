package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.PosInfo;
import javafx.geometry.Pos;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class OldGoodIdIndexFile extends Thread{

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int concurrentNum;

    public OldGoodIdIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int concurrentNum) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.concurrentNum = concurrentNum;
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
        generateGoodIdIndex();
        System.out.println("goodid build index " + " work end!");
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
            System.out.println("index " + index + " file by goodid" + " start.");
            Map<String, TreeMap<Long, PosInfo>> goodIndex = new TreeMap<String, TreeMap<Long, PosInfo>>();
            TreeMap<String, Long> twoIndexMap = new TreeMap<String, Long>();
            //for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            try {
                FileInputStream order_records = new FileInputStream(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + index);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File file = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_GOODID + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                String str = null;
                long count = 0;
                while ((str = order_br.readLine()) != null) {
                    String orderid = null;
                    String goodid = null;
                    String[] keyValues = str.split("\t");
                    for (int j = 0; j < keyValues.length; j++) {
                        String[] keyValue = keyValues[j].split(":");

                        if ("orderid".equals(keyValue[0])) {
                            orderid = keyValue[1];
                        } else if ("goodid".equals(keyValue[0])) {
                            goodid = keyValue[1];
                            if (!goodIndex.containsKey(goodid)) {
                                goodIndex.put(goodid, new TreeMap<Long, PosInfo>());
                            }
                        }
                        if (orderid != null && goodid != null) {
                            PosInfo posInfo = new PosInfo(count, str.getBytes().length);
                            goodIndex.get(goodid).put(Long.valueOf(orderid), posInfo);
                        }
                    }
                    count += str.getBytes().length + 1;
                }

                int towIndexSize = (int) Math.sqrt(goodIndex.size());
                FileConstant.goodIdIndexRegionSizeMap.put(index, towIndexSize);
                count = 0;
                long position = 0;
                Iterator iterator = goodIndex.entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry entry = (Map.Entry) iterator.next();
                    String key = (String) entry.getKey();
                    Map<String, PosInfo> val = (Map<String, PosInfo>) entry.getValue();
                    StringBuilder content = new StringBuilder(key + ":");
                    //String content = key + ":";
                    Iterator iteratorOrders = val.entrySet().iterator();
                    while (iteratorOrders.hasNext()) {
                        Map.Entry orderEntry = (Map.Entry) iteratorOrders.next();
                        PosInfo pos = (PosInfo) orderEntry.getValue();
                        content.append(pos.toString());
                        content.append("|");
                    }
                    val.clear();
                    bufferedWriter.write(content.toString() + '\n');

                    if (count%towIndexSize == 0) {
                        //PosInfo posInfo = new PosInfo(position, content.toString().getBytes().length);
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

//    public static long bytes2Long(byte[] byteNum) {
//        long num = 0;
//        for (int ix = 0; ix < 8; ++ix) {
//            num <<= 8;
//            num |= (byteNum[ix] & 0xff);
//        }
//        return num;
//    }
}
