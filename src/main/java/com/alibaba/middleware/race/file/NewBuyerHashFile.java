package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class NewBuyerHashFile extends Thread{

    private Collection<String> buyerFiles;
    private Collection<String> storeFolders;
    private int                nums;
    private CountDownLatch countDownLatch;

    public NewBuyerHashFile(Collection<String> buyerFiles, Collection<String> storeFolders, int nums, CountDownLatch countDownLatch) {
        this.buyerFiles = buyerFiles;
        this.storeFolders = storeFolders;
        this.nums = nums;
        this.countDownLatch = countDownLatch;
    }

    //读取所有买家文件，按照买家号hash到多个小文件中,生成到第二块磁盘中
    public void generateBuyerHashFile() {

        try {
            BufferedWriter[] bufferedWriters = new BufferedWriter[nums];

            for (int i = 0; i < nums; i++) {
                File file = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_BUYER_HASH + i);
                FileWriter fw = new FileWriter(file);
                bufferedWriters[i] = new BufferedWriter(fw);
            }

            CountDownLatch multiHashLatch = new CountDownLatch(buyerFiles.size());
            for (String file : buyerFiles) {               
                new MultiHash(file, multiHashLatch, "buyerid", bufferedWriters).start();
            }            
            multiHashLatch.await();

            for (int i = 0; i < nums; i++) {
                bufferedWriters[i].close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        generateBuyerHashFile();
        System.out.println("buyer file hash end~");
        countDownLatch.countDown();
    }
    
    private class MultiHash extends Thread{
        private String file;
        private CountDownLatch countDownLatch;
        private String type;
        private BufferedWriter[] bufferedWriters;
        
        public MultiHash(String file, CountDownLatch countDownLatch, String type, BufferedWriter[] bufferedWriters){
            this.file = file;
            this.countDownLatch = countDownLatch;
            this.type = type;
            this.bufferedWriters = bufferedWriters;
        }
        
        @Override
        public void run(){
            System.out.println(file + "hash file by " + type + " start.");
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

                String str = null;
                String buyerid = null;
                int hashFileIndex;
                while ((str = br.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        String[] keyValue = stringTokenizer.nextToken().split(":");
                        if (!KeyCache.buyerKeyCache.containsKey(keyValue[0])) {
                            KeyCache.buyerKeyCache.put(keyValue[0],
                                    KeyCache.EMPTY_OBJECT);
                        }
                        if (type.equals(keyValue[0])) {
                            buyerid = keyValue[1];
                            hashFileIndex = (int) (Math.abs(buyerid.hashCode()) % nums);
                            synchronized (bufferedWriters[hashFileIndex]) {
                                bufferedWriters[hashFileIndex].write(str + '\n');
                            }
                        }
                    }
                }
                br.close();
                countDownLatch.countDown();
                System.out.println(type + "hash file end :" + file);
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
