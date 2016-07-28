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
public class NewGoodHashFile extends Thread{

    private Collection<String> goodFiles;
    private Collection<String> storeFolders;
    private int                nums;
    private CountDownLatch countDownLatch;

    public NewGoodHashFile(Collection<String> goodFiles, Collection<String> storeFolders, int nums, CountDownLatch countDownLatch) {
        this.goodFiles = goodFiles;
        this.storeFolders = storeFolders;
        this.nums = nums;
        this.countDownLatch = countDownLatch;
    }

    //读取所有商品文件，按照商品号hash到多个小文件中, 生成到第一块磁盘中
    public void generateGoodHashFile() {

        try {
            BufferedWriter[] bufferedWriters = new BufferedWriter[nums];

            for (int i = 0; i < nums; i++) {
                File file = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_GOOD_HASH + i);
                FileWriter fw = new FileWriter(file);
                bufferedWriters[i] = new BufferedWriter(fw);
            }
            
            CountDownLatch multiHashLatch = new CountDownLatch(goodFiles.size());
            for (String file : goodFiles) {               
                new MultiHash(file, multiHashLatch, "goodid", bufferedWriters).start();
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
        generateGoodHashFile();
        System.out.println("good file hash end~");
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
                String goodid = null;
                int hashFileIndex;
                while ((str = br.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        String[] keyValue = stringTokenizer.nextToken().split(":");
                        if (!KeyCache.goodKeyCache.containsKey(keyValue[0])) {
                            KeyCache.goodKeyCache.put(keyValue[0],
                                    KeyCache.EMPTY_OBJECT);
                        }
                        if (type.equals(keyValue[0])) {
                            goodid = keyValue[1];
                            hashFileIndex = (int) (Math.abs(goodid.hashCode()) % nums);
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
