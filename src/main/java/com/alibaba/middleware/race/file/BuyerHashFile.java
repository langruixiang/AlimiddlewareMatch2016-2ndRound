package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.FilePosition;

import java.io.*;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class BuyerHashFile extends Thread{

    private Collection<String> buyerFiles;
    private Collection<String> storeFolders;
    private int                nums;
    private CountDownLatch countDownLatch;

    public BuyerHashFile(Collection<String> buyerFiles, Collection<String> storeFolders, int nums, CountDownLatch countDownLatch) {
        this.buyerFiles = buyerFiles;
        this.storeFolders = storeFolders;
        this.nums = nums;
        this.countDownLatch = countDownLatch;
    }

    //读取所有买家文件，按照买家号hash到多个小文件中,生成到第二块磁盘中
    public void generateBuyerHashFile() {

        try {

            int count = 0;
            for (String buyerFile : buyerFiles) {
                FileInputStream buyer_records = new FileInputStream(buyerFile);
                BufferedReader buyer_br = new BufferedReader(new InputStreamReader(buyer_records));

                String str = null;
                long buyerid = 0;
                int hashFileIndex;
                long position = 0;
                while ((str = buyer_br.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        if (!KeyCache.buyerKeyCache.containsKey(key)) {
                            KeyCache.buyerKeyCache.put(key, 0);
                        }
                        if ("buyerid".equals(key)) {
                            buyerid = value.hashCode();
                            hashFileIndex = (int) (Math.abs(buyerid) % nums);
                            FilePosition filePosition = new FilePosition(buyerFile, position);
                            OneIndexCache.buyerOneIndexCache.put(value, filePosition);
                            position += str.getBytes().length + 1;
                        }
                    }
                }
                System.out.println("buyer hash file " + count++);
                buyer_br.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        generateBuyerHashFile();
        System.out.println("buyer file hash end~");
        countDownLatch.countDown();
    }

}
