package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class GoodIdIndexFile extends Thread{

    private Map<String, TreeMap<String, Long>> goodIndex = new TreeMap<String, TreeMap<String, Long>>();

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int index;

    public GoodIdIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int index) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.index = index;
    }

    //订单文件按照goodid生成索引文件，存放到第三块磁盘上
    public void generateGoodIdIndex() {

        //for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            goodIndex.clear();

            try {
                FileInputStream order_records = new FileInputStream(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + index);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File file = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_GOODID + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                File twoIndexfile = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_TWO_INDEXING_BY_GOODID + index);
                FileWriter twoIndexfw = new FileWriter(twoIndexfile);
                BufferedWriter twoIndexBW = new BufferedWriter(twoIndexfw);

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
                                goodIndex.put(goodid, new TreeMap<String, Long>());
                            }
                        }
                        if (orderid != null && goodid != null) {
                            goodIndex.get(goodid).put(orderid, count);
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
                    Map<String, Long> val = (Map<String, Long>) entry.getValue();
                    String content = key + ":";
                    Iterator iteratorOrders = val.entrySet().iterator();
                    while (iteratorOrders.hasNext()) {
                        Map.Entry orderEntry = (Map.Entry) iteratorOrders.next();
                        Long pos = (Long)orderEntry.getValue();
                        content = content + pos + "|";
                    }
                    val.clear();

                    bufferedWriter.write(content + '\n');

                    if (count%towIndexSize == 0) {
                        twoIndexBW.write(key+":");
                        twoIndexBW.write(String.valueOf(position) + '\n');
                        //twoIndexBW.newLine();
                    }
                    position += content.getBytes().length + 1;
                    //bufferedWriter.newLine();

                    count++;
                }
                goodIndex.clear();
                bufferedWriter.flush();
                bufferedWriter.close();
                twoIndexBW.flush();
                twoIndexBW.close();
                order_br.close();
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
                hashDownLatch.await();//等待上一个任务的完成
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        generateGoodIdIndex();
        buildIndexCountLatch.countDown();//完成工作，计数减一
        System.out.println("goodid build index " + index + " work end!");
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
