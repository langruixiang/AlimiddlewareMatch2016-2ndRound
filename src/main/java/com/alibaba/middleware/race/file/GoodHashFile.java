package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.cache.FileNameCache;
import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.model.FilePosition;

import java.io.*;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class GoodHashFile extends Thread{

    private CountDownLatch     waitCountDownLatch;
    private Collection<String> goodFiles;
    private Collection<String> storeFolders;
    private int                nums;
    private CountDownLatch countDownLatch;
    private int                fileBeginNum;

    public GoodHashFile(CountDownLatch waitCountDownLatch, Collection<String> goodFiles, Collection<String> storeFolders, int nums, CountDownLatch countDownLatch, int fileBeginNum) {
        this.waitCountDownLatch = waitCountDownLatch;
        this.goodFiles = goodFiles;
        this.storeFolders = storeFolders;
        this.nums = nums;
        this.countDownLatch = countDownLatch;
        this.fileBeginNum = fileBeginNum;
    }

    //读取所有商品文件，按照商品号hash到多个小文件中, 生成到第一块磁盘中
    public void generateGoodHashFile() {

        try {
            int count = 0;
            for (String goodFile : goodFiles) {
                FileNameCache.fileNameMap.put(fileBeginNum, goodFile);
                FileInputStream good_records = new FileInputStream(goodFile);
                BufferedReader good_br = new BufferedReader(new InputStreamReader(good_records));
//                RandomAccessFile ranRaf = new RandomAccessFile(new File(goodFile), "r");
//                RandomFile.randomFileMap.put(goodFile, ranRaf);

                String str = null;
                long goodid = 0;
                int hashFileIndex;
                long position = 0;
                while ((str = good_br.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        if (!KeyCache.goodKeyCache.containsKey(key)) {
                            KeyCache.goodKeyCache.put(key, 0);
                        }
                        if ("goodid".equals(key)) {
                            goodid = value.hashCode();
                            hashFileIndex = (int) (Math.abs(goodid) % nums);
                            FilePosition filePosition = new FilePosition(fileBeginNum, position);
                            OneIndexCache.goodOneIndexCache.put(value, filePosition);
                            position += str.getBytes().length + 1;
                        }
                    }
                }
                good_br.close();
                fileBeginNum++;
                System.out.println("good hash FIle " + count++);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        try {
            waitCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("good file hash begin~");
        generateGoodHashFile();
        System.out.println("good file hash end~");
        countDownLatch.countDown();
    }
}
