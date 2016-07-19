package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.Collection;
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
            BufferedWriter[] bufferedWriters = new BufferedWriter[nums];

            for (int i = 0; i < nums; i++) {
                File file = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_BUYER_HASH + i);
                FileWriter fw = new FileWriter(file);
                bufferedWriters[i] = new BufferedWriter(fw);
            }

            for (String buyerFile : buyerFiles) {
                FileInputStream buyer_records = new FileInputStream(buyerFile);
                BufferedReader buyer_br = new BufferedReader(new InputStreamReader(buyer_records));

                String str = null;
                long buyerid = 0;
                int hashFileIndex;
                while ((str = buyer_br.readLine()) != null) {
                    String[] keyValues = str.split("\t");
                    for (int i = 0; i < keyValues.length; i++) {
                        String[] keyValue = keyValues[i].split(":");
                        if ("buyerid".equals(keyValue[0])) {
                            buyerid = keyValue[1].hashCode();
                            hashFileIndex = (int) (Math.abs(buyerid) % nums);
                            bufferedWriters[hashFileIndex].write(str + '\n');
                            //bufferedWriters[hashFileIndex].newLine();
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
        generateBuyerHashFile();
        System.out.println("good file hash end~");
        countDownLatch.countDown();
    }

}
