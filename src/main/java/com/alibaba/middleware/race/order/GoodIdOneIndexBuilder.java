package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class GoodIdOneIndexBuilder extends Thread {

    private Collection<String> orderFiles;
    private int indexFileNum;
    private CountDownLatch builderLatch;
    private int fileBeginNo;

    public GoodIdOneIndexBuilder(Collection<String> orderFiles,
            int indexFileNum, CountDownLatch builderLatch, int fileBeginNum) {
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
                bufferedWriters[i] = new BufferedWriter(new FileWriter(
                        Config.THIRD_DISK_PATH
                                + FileConstant.FILE_INDEX_BY_GOODID + i));
            }

            // 每个orderFile 分配一个task
            CountDownLatch tasksLatch = new CountDownLatch(orderFiles.size());
            for (String orderFile : orderFiles) {
                new SingleFileBuildTask(orderFile, tasksLatch, bufferedWriters,
                        fileBeginNo).start();
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
        System.out.println("GoodIdOneIndexBuilder work end! time : "
                + (System.currentTimeMillis() - startTime));
    }

    private class SingleFileBuildTask extends Thread {
        private String orderFile;
        private CountDownLatch tasksLatch;
        private BufferedWriter[] bufferedWriters;
        private int fileNum;

        public SingleFileBuildTask(String orderFile, CountDownLatch tasksLatch,
                BufferedWriter[] bufferedWriters, int fileNum) {
            this.orderFile = orderFile;
            this.tasksLatch = tasksLatch;
            this.bufferedWriters = bufferedWriters;
            this.fileNum = fileNum;
        }

        @Override
        public void run() {
            try {
                FileInputStream orderRecords = new FileInputStream(orderFile);
                BufferedReader orderBr = new BufferedReader(
                        new InputStreamReader(orderRecords));

                String line = null;
                while ((line = orderBr.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(line,
                            "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        StringTokenizer keyValue = new StringTokenizer(
                                stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();

                        if ("goodid".equals(key)) {
                            String goodId = value;
                            int hashFileIndex = (int) (Math.abs(goodId
                                    .hashCode()) % indexFileNum);
                            synchronized (bufferedWriters[hashFileIndex]) {
                                bufferedWriters[hashFileIndex]
                                        .write(line + '\n');
                            }
                            break;
                        }
                    }
                }
                orderBr.close();
                tasksLatch.countDown();
                System.out.println("goodid" + " SingleFileBuildTask end :"
                        + orderFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
