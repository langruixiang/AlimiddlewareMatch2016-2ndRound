package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class OrderIdIndexFile extends Thread{

    private Map<String, Long> orderIndex = new TreeMap<String, Long>();

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int index;

    public OrderIdIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int index) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.index = index;
    }

    //订单文件按照goodid生成索引文件，存放到第三块磁盘上
    public void generateOrderIdIndex() {

        //for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            orderIndex.clear();

            try {
                FileInputStream order_records = new FileInputStream(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_INDEX_BY_ORDERID + index);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File file = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_ORDERID + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                File twoIndexfile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_TWO_INDEXING_BY_ORDERID + index);
                FileWriter twoIndexfw = new FileWriter(twoIndexfile);
                BufferedWriter twoIndexBW = new BufferedWriter(twoIndexfw);

                String str = null;
                long count = 0;
                String orderid = null;
                while ((str = order_br.readLine()) != null) {
                    String[] keyValues = str.split("\t");
                    for (int j = 0; j < keyValues.length; j++) {
                        String[] keyValue = keyValues[j].split(":");

                        if ("orderid".equals(keyValue[0])) {
                            orderid = keyValue[1];
                            orderIndex.put(orderid, count);
                            break;
                        }
                    }
                    count += str.getBytes().length + 1;
                }

                int towIndexSize = (int) Math.sqrt(orderIndex.size());
                FileConstant.orderIdIndexRegionSizeMap.put(index, towIndexSize);
                count = 0;
                long position = 0;
                Iterator iterator = orderIndex.entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry entry = (Map.Entry) iterator.next();
                    String key = (String) entry.getKey();
                    Long val = (Long) entry.getValue();
                    String content = key + ":";
                    content = content + val;
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
        generateOrderIdIndex();
        buildIndexCountLatch.countDown();//完成工作，计数减一
        System.out.println("orderid build index " + index + " work end!");
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
