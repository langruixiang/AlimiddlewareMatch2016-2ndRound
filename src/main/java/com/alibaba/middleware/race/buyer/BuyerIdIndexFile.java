package com.alibaba.middleware.race.buyer;

import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class BuyerIdIndexFile extends Thread{

    private TreeMap<String, List<Long>> buyerIndex = new TreeMap<String, List<Long>>();

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int index;

    public BuyerIdIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int index) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.index = index;
    }

    //订单文件按照buyerid生成索引文件，存放到第二块磁盘上
    public void generateBuyerIdIndex() {

        //for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            buyerIndex.clear();

            try {
                FileInputStream order_records = new FileInputStream(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_INDEX_BY_BUYERID + index);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File file = new File(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_BUYERID + index);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                File twoIndexfile = new File(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_TWO_INDEXING_BY_BUYERID + index);
                FileWriter twoIndexfw = new FileWriter(twoIndexfile);
                BufferedWriter twoIndexBW = new BufferedWriter(twoIndexfw);

                String str = null;
                long count = 0;

                while ((str = order_br.readLine()) != null) {
                    String buyerid = null;
                    String createtime = null;
                    String[] keyValues = str.split("\t");
                    for (int j = 0; j < keyValues.length; j++) {
                        String[] keyValue = keyValues[j].split(":");

                        if ("buyerid".equals(keyValue[0])) {
                            buyerid = keyValue[1];
                        } else if ("createtime".equals(keyValue[0])) {
                            createtime = keyValue[1];
                        }
                        if (buyerid != null && createtime != null) {
                            String newKey = buyerid + "_" + createtime;
                            if (!buyerIndex.containsKey(newKey)) {
                                buyerIndex.put(newKey, new ArrayList<Long>());
                            }
                            buyerIndex.get(newKey).add(count);
                            break;
                        }
                    }
                    count += str.getBytes().length + 1;
                }

                int twoIndexSize = (int) Math.sqrt(buyerIndex.size());
                FileConstant.buyerIdIndexRegionSizeMap.put(index, twoIndexSize);
                count = 0;
                long position = 0;
                Iterator iterator = buyerIndex.descendingMap().entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry entry = (Map.Entry) iterator.next();
                    String key = (String) entry.getKey();
                    List<Long> val = (List<Long>) entry.getValue();
                    String content = key + ":";
                    for (Long num : val) {
                        content = content + num + "|";
                    }
                    bufferedWriter.write(content + '\n');

                    if (count%twoIndexSize == 0) {
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
                hashDownLatch.await(); //等待上一个任务的结束
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        generateBuyerIdIndex();
        buildIndexCountLatch.countDown();
        System.out.println("buyerid build index " + index + " work end!");
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
