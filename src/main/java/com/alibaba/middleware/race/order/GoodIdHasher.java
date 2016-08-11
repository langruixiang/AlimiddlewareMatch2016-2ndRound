package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.constant.IndexConstant;

import java.io.*;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

/**
 * 按商品ID将订单hash成多个小文件(未排序)并存储
 * 
 * 存放位置：第三个硬盘
 * 
 * @author jiangchao
 */
public class GoodIdHasher extends Thread {

    private Collection<String> orderFiles;
    private int indexFileNum;
    private CountDownLatch builderLatch;
    private int fileBeginNo;

    public GoodIdHasher(Collection<String> orderFiles, int indexFileNum,
            CountDownLatch builderLatch, int fileBeginNum) {
        this.orderFiles = orderFiles;
        this.indexFileNum = indexFileNum;
        this.builderLatch = builderLatch;
        this.fileBeginNo = fileBeginNum;
    }

    // 读取所有订单文件，把每条记录按照goodid hash到对应新的记录文件
    public void build() {
        try {
            BufferedWriter[] bufferedWriters = new BufferedWriter[indexFileNum];
            for (int i = 0; i < indexFileNum; i++) {
                bufferedWriters[i] = new BufferedWriter(
                        new FileWriter(
                                Config.THIRD_DISK_PATH
                                        + FileConstant.UNSORTED_GOOD_ID_HASH_FILE_PREFIX
                                        + i));
            }

            // 每个orderFile 分配一个task
            CountDownLatch tasksLatch = new CountDownLatch(orderFiles.size());
            for (String orderFile : orderFiles) {
                new SingleFileBuildTask(orderFile, tasksLatch, bufferedWriters, fileBeginNo).start();
                fileBeginNo++;
            }
            tasksLatch.await();

            for (int i = 0; i < indexFileNum; i++) {
                bufferedWriters[i].close();
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        long startTime = System.currentTimeMillis();
        build();
        builderLatch.countDown();
        System.out.printf("GoodIdHasher work end! Used time：%d End time : %d %n",
                        System.currentTimeMillis() - startTime,
                        System.currentTimeMillis() - OrderSystemImpl.constructStartTime);
    }

    private class SingleFileBuildTask extends Thread {
        private String orderFile;
        private CountDownLatch tasksLatch;
        private BufferedWriter[] bufferedWriters;

        public SingleFileBuildTask(String orderFile, CountDownLatch tasksLatch,
                BufferedWriter[] bufferedWriters, int fileNum) {
            this.orderFile = orderFile;
            this.tasksLatch = tasksLatch;
            this.bufferedWriters = bufferedWriters;
        }

        @Override
        public void run() {
            BufferedReader orderBr = null;
            try {
                FileInputStream orderRecords = new FileInputStream(orderFile);
                orderBr = new BufferedReader(new InputStreamReader(orderRecords));

                String line = null;
                while ((line = orderBr.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(line, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();

                        if (IndexConstant.GOOD_ID.equals(key)) {
                            String goodId = value;
                            int hashFileIndex = (int) (Math.abs(goodId.hashCode()) % indexFileNum);
                            synchronized (bufferedWriters[hashFileIndex]) {
                                bufferedWriters[hashFileIndex].write(line + '\n');
                            }
                            break;
                        }
                    }
                }
                tasksLatch.countDown();
                System.out.println(IndexConstant.GOOD_ID + " SingleFileBuildTask end :" + orderFile);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (orderBr != null) {
                    try {
                        orderBr.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
