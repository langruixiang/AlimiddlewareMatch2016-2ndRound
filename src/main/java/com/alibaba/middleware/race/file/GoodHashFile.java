package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class GoodHashFile extends Thread{

    private Collection<String> goodFiles;
    private Collection<String> storeFolders;
    private int                nums;

    public GoodHashFile(Collection<String> goodFiles, Collection<String> storeFolders, int nums) {
        this.goodFiles = goodFiles;
        this.storeFolders = storeFolders;
        this.nums = nums;
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

            for (String goodFile : goodFiles) {
                FileInputStream good_records = new FileInputStream(goodFile);
                BufferedReader good_br = new BufferedReader(new InputStreamReader(good_records));

                String str = null;
                long goodid = 0;
                int hashFileIndex;
                while ((str = good_br.readLine()) != null) {
                    String[] keyValues = str.split("\t");
                    for (int i = 0; i < keyValues.length; i++) {
                        String[] keyValue = keyValues[i].split(":");
                        if ("goodid".equals(keyValue[0])) {
                            goodid = keyValue[1].hashCode();
                            hashFileIndex = (int) (Math.abs(goodid) % nums);
                            bufferedWriters[hashFileIndex].write(str);
                            bufferedWriters[hashFileIndex].newLine();
                            break;
                        }
                    }
                }
            }

            for (int i = 0; i < nums; i++) {
                bufferedWriters[i].close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        generateGoodHashFile();
    }
}
