package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class GoodIndexFile extends Thread{

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int index;

    public GoodIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int index) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.index = index;
    }

    //Good文件按照Goodid生成索引文件，存放到第二块磁盘上
    public void generateGoodIndex() {

            try {
                FileInputStream good_records = new FileInputStream(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_GOOD_HASH + index);
                BufferedReader good_br = new BufferedReader(new InputStreamReader(good_records));

                String str = null;
                long position = 0;
                while ((str = good_br.readLine()) != null) {
                    String goodid = null;
                    String[] keyValues = str.split("\t");
                    for (int j = 0; j < keyValues.length; j++) {
                        String[] keyValue = keyValues[j].split(":");

                        if ("goodid".equals(keyValue[0])) {
                            goodid = keyValue[1];
                            OneIndexCache.goodOneIndexCache.put(goodid, position);
                            break;
                        }
                    }
                    position += str.getBytes().length + 1;
                }
                good_br.close();
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
        generateGoodIndex();
        buildIndexCountLatch.countDown();
        System.out.println("good build index " + index + " work end!");
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
