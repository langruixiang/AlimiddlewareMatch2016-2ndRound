package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class GoodQuery {
    public static Good findGoodById(String goodId, int index) {
        if (goodId == null || goodId.isEmpty()) return null;
        System.out.println("==========:"+goodId + " index:" + index);
        Good good = new Good();
        try {

            File rankFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_GOOD_HASH + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "rw");

            String str = null;

            //1.查找索引
            long position = 0;
            if (!OneIndexCache.goodOneIndexCache.containsKey(goodId)) {
                return null;
            } else {
                position = OneIndexCache.goodOneIndexCache.get(goodId);
            }
            //System.out.println(position);

            //2.按行读取内容
            hashRaf.seek(position);
            String oneIndex = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
            if (oneIndex == null) return null;

            //3.将字符串转成buyer对象
            String[] keyValues = oneIndex.split("\t");
            for (int i = 0; i < keyValues.length; i++) {
                String[] strs = keyValues[i].split(":");
                KeyValue kv = new KeyValue();
                kv.setKey(strs[0]);
                kv.setValue(strs[1]);
                good.getKeyValues().put(strs[0], kv);
            }
            good.setId(goodId);
            hashRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return good;
    }

    public static void main(String args[]) {

        //BuyerIdIndexFile.generateBuyerIdIndex();
        //findByBuyerId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }

    private static RandomAccessFile[] goodHashFiles;

    public static void initGoodHashFiles() throws FileNotFoundException {
        goodHashFiles = new RandomAccessFile[FileConstant.FILE_NUMS];
        for (int i = 0; i < FileConstant.FILE_NUMS;++i) {
            File rankFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_GOOD_HASH + i);
            goodHashFiles[i] = new RandomAccessFile(rankFile, "rw");
        }
    }

    public static String getGoodLine(String goodId) throws UnsupportedEncodingException, IOException {
        int hashFileIndex = (int) (Math.abs(goodId.hashCode()) % FileConstant.FILE_NUMS);
        RandomAccessFile hashRaf = goodHashFiles[hashFileIndex];

        //1.查找索引
        long position = 0;
        if (!OneIndexCache.goodOneIndexCache.containsKey(goodId)) {
            return null;
        } else {
            position = OneIndexCache.goodOneIndexCache.get(goodId);
        }
        //System.out.println(position);

        //2.按行读取内容
        hashRaf.seek(position);
        String oneIndex = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
        return oneIndex;
    }
    
    public static void closeGoodHashFiles() throws IOException {
        for (int i = 0; i < FileConstant.FILE_NUMS;++i) {
            goodHashFiles[i].close();
        }
        goodHashFiles = null;
    }
}
