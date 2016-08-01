package com.alibaba.middleware.race;

import java.io.*;
import java.util.Random;

import com.alibaba.middleware.race.util.RandomAccessFileUtil;
import com.alibaba.middleware.race.util.StringUtil;

/**
 * Created by jiangchao on 2016/7/26.
 */
public class ProduceData {
    public static void produceOrderData(int lastNumber) {

        try {
            String         str           = null;
            String         buyerid       = null;
            String         goodid        = null;
            Long           orderid       = null;

            Random random = new Random(100);
            String pressName = "order.3.";
            long count = 0;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < 40; i++) {
                File file = new File(pressName + i);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);
                while (count < 1000000) {
                    lastNumber++;
                    goodid = "abc" + random.nextInt(10000);
                    buyerid = "def" + random.nextInt(10000);
                    String line = "orderid:" + lastNumber + "\tcreatetime:1476305929\tbuyerid:" + buyerid + "\tgoodid:" + goodid + "\tamount:1\tdone:true\ta_o_4699:5343\ta_o_25949:289fe8a6-1959-48f1-9b0a-79e72b1d2827\ta_o_9238:46876674-307e-4161-b164-43a2c7a368c7\ta_o_18188:true\ta_o_5497:7\ta_o_12490:-47632\n";
                    bufferedWriter.write(line);
                    count++;
                }
                bufferedWriter.flush();
                bufferedWriter.close();
                count = 0;
            }
            System.out.println(System.currentTimeMillis() - startTime);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public ProduceData() throws FileNotFoundException {
    }
    
    public static void main(String[] args) {
        try {
//            proceduceLargeFile();
            testRandomAccessFileReadLine();
//            testRandomAccessFileUtilReadLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void proceduceLargeFile() throws IOException {
        long startTime = System.currentTimeMillis();
        File file = new File("largeFile.txt");
        FileWriter fw = new FileWriter(file);
        BufferedWriter bufferedWriter = new BufferedWriter(fw);
        int count = 0;
        while (count++ < 50000) {
            String line = StringUtil.genRandomString(2054) + "fa割发代首" + "\n";
            bufferedWriter.write(line);
            if (count == 1) {
                System.out.println("line.length = " + line.getBytes().length);
            }
        }
        bufferedWriter.flush();
        bufferedWriter.close();
        System.out.println("Total used time : " + (System.currentTimeMillis() - startTime));
    }
    
    public static void testRandomAccessFileReadLine() throws IOException {
        long startTime = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile("largeFile.txt", "r");
        String line = null;
        boolean flg = false;
        while((line = raf.readLine()) != null) {
            if (!flg) {
                flg = !flg;
                System.out.println(line);
                System.out.println(line.getBytes().length);
            }
        }
        raf.close();
        System.out.println("Total used time : " + (System.currentTimeMillis() - startTime));
    }

    public static void testRandomAccessFileUtilReadLine() throws IOException {
        long startTime = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile("largeFile.txt", "r");
        String line = null;
        boolean flg = false;
        long offset = 0;
        while((line = RandomAccessFileUtil.readLine(raf, offset)) != null) {
            offset += (line.getBytes().length + 1);
            if (!flg) {
                flg = !flg;
                System.out.println(line);
                System.out.println(line.getBytes().length);
            }
        }
        raf.close();
        System.out.println("Total used time : " + (System.currentTimeMillis() - startTime));
    }
}