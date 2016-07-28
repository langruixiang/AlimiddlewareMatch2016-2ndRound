package com.alibaba.middleware.race;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

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
    
    public static void main(String argus[]) {
        try {
            produceLargeFile();//666s
//            testBufferedReader();//833s 1130s 765s
//            testRandomAccessFile();//17621s 20063s 19285s
            testRandomAccessFile2();// 2501s 2610s 2549s
            testmappedByteBufferRead2();//2995s 2920s 2675s
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void produceLargeFile() throws IOException {
        long startTime = System.currentTimeMillis();
        
        String fileName = "largeFile.txt";
        long count = 0;
        File file = new File(fileName);
        FileWriter fw = new FileWriter(file);
        BufferedWriter bufferedWriter = new BufferedWriter(fw);
        long size = 0;
        String line = null;
        while (count++ < 200000) {
            line = "orderid:123456" + "\tcreatetime:1476305929\tbuyerid:ghdgfwgf" + "\tgoodid:gfjghdtet" + "\tamount:1\tdone:true\ta_o_4699:5343\ta_o_25949:289fe8a6-1959-48f1-9b0a-79e72b1d2827\ta_o_9238:46876674-307e-4161-b164-43a2c7a368c7\ta_o_18188:true\ta_o_5497:7\ta_o_12490:-47632\n";
            bufferedWriter.write(line);
            size += line.getBytes().length;
        }
        System.out.println("line size : " + size / 100000);
        System.out.println("file size : " + size);
        bufferedWriter.flush();
        bufferedWriter.close();

        System.out.println("time used : " + (System.currentTimeMillis() - startTime) + "s");
//        line size : 480
//        file size : 48000000
//        time used : 666s
    }
    
    public static void testBufferedReader() throws IOException {
        System.out.println("testBufferedReader");
        long startTime = System.currentTimeMillis();
        
        BufferedReader br = new BufferedReader(new FileReader("largeFile.txt"));  
        String line = null;
        while((line = br.readLine()) != null){
            System.out.println(line.getBytes().length);
        }
        br.close();

        System.out.println("time used : " + (System.currentTimeMillis() - startTime) + "s");
    }
    
    public static void testRandomAccessFile() throws IOException {
        System.out.println("testRandomAccessFile");
        long startTime = System.currentTimeMillis();
        
        RandomAccessFile raf = new RandomAccessFile("largeFile.txt", "r");
        String line = null;
        while((line = raf.readLine()) != null){
            System.out.println(line.getBytes().length);
        }
        raf.close();

        System.out.println("time used : " + (System.currentTimeMillis() - startTime) + "s");
    }
    
    public static void testRandomAccessFile2() throws IOException {
        System.out.println("testRandomAccessFile2");
        long startTime = System.currentTimeMillis();
        
        RandomAccessFile raf = new RandomAccessFile("largeFile.txt", "r");
        String line = null;
        long offset = 0;
        int length = 240;
        while(offset < raf.length()){
            byte[] lineBytes = new byte[length];
            raf.read(lineBytes);
            line = new String(lineBytes);
            System.out.println(line.getBytes().length);
            offset += length;
        }
        raf.close();

        System.out.println("time used : " + (System.currentTimeMillis() - startTime) + "s");
    }
    
    public static void testmappedByteBufferRead2() throws IOException {
        System.out.println("testRandomAccessFile");
        long startTime = System.currentTimeMillis();
        
        RandomAccessFile raf = new RandomAccessFile("largeFile.txt", "r");
        FileChannel channel = raf.getChannel();  

        String line = null;
        int offset = 0;
        int length = 240;
        int count = 0;
        while(offset < channel.size()){
            MappedByteBuffer buffer = channel.map(  
                    FileChannel.MapMode.READ_ONLY, offset, length);
            byte[] lineBytes = new byte[length];
            buffer.get(lineBytes);
            line = new String(lineBytes);
            System.out.println(line.getBytes().length);
            offset += length;
            if(count++ % 5000 == 0) {
                System.gc();
            }
        }
        channel.close();
        raf.close();

        System.out.println("time used : " + (System.currentTimeMillis() - startTime) + "s");
    }
}
