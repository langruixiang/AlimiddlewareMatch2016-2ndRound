package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
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
        Map<String, String> orderRankMap = new TreeMap<String, String>();

        //for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            try {
                FileInputStream order_records = new FileInputStream(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + index);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File fileRank = new File(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_RANK_BY_GOODID + index);
                FileWriter fwRank = new FileWriter(fileRank);
                BufferedWriter rankBW = new BufferedWriter(fwRank);

                String rankStr = null;
                while ((rankStr = order_br.readLine()) != null) {
                    String orderid = null;
                    String goodid = null;
                    String[] keyValues = rankStr.split("\t");
                    for (int j = 0; j < keyValues.length; j++) {
                        String[] keyValue = keyValues[j].split(":");

                        if ("orderid".equals(keyValue[0])) {
                            orderid = keyValue[1];
                        } else if ("goodid".equals(keyValue[0])) {
                            goodid = keyValue[1];
                        }
                        if (orderid != null && goodid != null) {
                            String key = goodid + "|" + orderid;
                            orderRankMap.put(key, rankStr);
                            break;
                        }
                    }
                }

                long position = 0;
                Iterator orderRankIterator = orderRankMap.entrySet().iterator();
                String preGoodid = null;
                while (orderRankIterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) orderRankIterator.next();
                    String key = (String) entry.getKey();
                    String val = (String) entry.getValue();
                    rankBW.write(val + '\n');

                    String[] keys = key.split("\\|");
                    String goodid = keys[0];
                    if (!OneIndexCache.goodidOneIndexCache.containsKey(goodid)) {
                        //goodIndex.put(goodid, position);
                        List<Long> positions = new ArrayList<Long>();
                        positions.add(position);
                        OneIndexCache.goodidOneIndexCache.put(goodid, positions);
                        if (preGoodid != null) {
                            Long length = position - OneIndexCache.goodidOneIndexCache.get(preGoodid).get(0) -1;
                            OneIndexCache.goodidOneIndexCache.get(preGoodid).add(length);
                        }
                        preGoodid = goodid;
                    }
                    position += val.getBytes().length + 1;
                }
                if (preGoodid != null) {
                    Long length = position - OneIndexCache.goodidOneIndexCache.get(preGoodid).get(0) -1;
                    OneIndexCache.goodidOneIndexCache.get(preGoodid).add(length);
                }

                rankBW.flush();
                rankBW.close();
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

}
