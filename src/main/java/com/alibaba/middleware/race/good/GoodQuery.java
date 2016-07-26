package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.PosInfo;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

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
                position = OneIndexCache.goodOneIndexCache.get(goodId).offset;
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

    private RandomAccessFile[] goodHashFiles;
    public FileChannel[] hashFileChannels;
    public MappedByteBuffer[] hashFileMappedByteBuffers;

    public void initGoodHashFiles() throws IOException {
        goodHashFiles = new RandomAccessFile[FileConstant.FILE_NUMS];
        hashFileChannels = new FileChannel[FileConstant.FILE_NUMS];
        hashFileMappedByteBuffers = new MappedByteBuffer[FileConstant.FILE_NUMS];
        for (int i = 0; i < FileConstant.FILE_NUMS;++i) {
            File rankFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_GOOD_HASH + i);
            goodHashFiles[i] = new RandomAccessFile(rankFile, "r");
            hashFileChannels[i] = goodHashFiles[i].getChannel();
            hashFileMappedByteBuffers[i] = hashFileChannels[i].map(MapMode.READ_ONLY, 0, hashFileChannels[i].size());
        }
    }

    public String getGoodLine(String goodId) throws UnsupportedEncodingException, IOException {
        int hashFileIndex = (int) (Math.abs(goodId.hashCode()) % FileConstant.FILE_NUMS);

        //1.查找索引
        PosInfo posInfo = null;
        if (!OneIndexCache.goodOneIndexCache.containsKey(goodId)) {
            return null;
        } else {
            posInfo = OneIndexCache.goodOneIndexCache.get(goodId);
        }
        //System.out.println(position);

        //2.按行读取内容
        MappedByteBuffer mappedByteBuffer = hashFileMappedByteBuffers[hashFileIndex];
        byte[] buffer = new byte[posInfo.length - 1];
        synchronized (mappedByteBuffer) {
            mappedByteBuffer.position(posInfo.offset);
            try {
                mappedByteBuffer.get(buffer);
            } catch (BufferUnderflowException e) {
                e.printStackTrace();
            }
        }
        return new String(buffer);
//        String oneIndex = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
    }
    
    public void closeGoodHashFiles() throws IOException {
        for (int i = 0; i < FileConstant.FILE_NUMS;++i) {
            goodHashFiles[i].close();
        }
        goodHashFiles = null;
    }
}
