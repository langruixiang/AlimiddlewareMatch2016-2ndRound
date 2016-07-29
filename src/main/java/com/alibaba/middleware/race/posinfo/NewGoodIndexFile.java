package com.alibaba.middleware.race.posinfo;

import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.PosInfo;

import java.io.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class NewGoodIndexFile extends Thread{

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int index;

    public NewGoodIndexFile(CountDownLatch hashDownLatch, CountDownLatch buildIndexCountLatch, int index) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.index = index;
    }

    //Good文件按照Goodid生成索引文件，存放到第二块磁盘上
    public void generateGoodIndex() {

            try {
                FileInputStream good_records = new FileInputStream(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_GOOD_HASH + index);
                BufferedReader good_br = new BufferedReader(new InputStreamReader(good_records));

                String line = null;
                int offset = 0;
                int length = 0;
                while ((line = good_br.readLine()) != null) {
                    int goodIdStartIndex = line.indexOf("goodid:") + 7;// 7 : "goodid:".length()
                    int goodIdEndIndex = line.indexOf('\t', goodIdStartIndex);
                    if (goodIdEndIndex < 0) {
                        goodIdEndIndex = line.indexOf('\n', goodIdStartIndex);
                    }
                    String goodId = line.substring(goodIdStartIndex, goodIdEndIndex);
                    length = line.getBytes().length;
                    NewOneIndexCache.goodOneIndexCache.put(goodId, new PosInfo(offset, length));
                    offset = offset + length + 1;
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
