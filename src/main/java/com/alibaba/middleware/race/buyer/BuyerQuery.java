package com.alibaba.middleware.race.buyer;

import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.file.PosInfo;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class BuyerQuery {
    public static Buyer findBuyerById(String buyerId, int index) {
        if (buyerId == null || buyerId.isEmpty()) return null;
        System.out.println("==========:"+buyerId + " index:" + index);
        Buyer buyer = new Buyer();
        try {

            File rankFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_BUYER_HASH + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "rw");

            String str = null;

            //1.查找索引
            long position = 0;
            if (!OneIndexCache.buyerOneIndexCache.containsKey(buyerId)) {
                return null;
            } else {
                position = OneIndexCache.buyerOneIndexCache.get(buyerId).offset;
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
                buyer.getKeyValues().put(strs[0], kv);
            }
            buyer.setId(buyerId);
            hashRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buyer;
    }
    
    public RandomAccessFile[] buyerHashFiles;
    public FileChannel[] hashFileChannels;
    public MappedByteBuffer[] hashFileMappedByteBuffers;

    public void initBuyerHashFiles() throws IOException {
        buyerHashFiles = new RandomAccessFile[FileConstant.FILE_NUMS];
        hashFileChannels = new FileChannel[FileConstant.FILE_NUMS];
        hashFileMappedByteBuffers = new MappedByteBuffer[FileConstant.FILE_NUMS];
        for (int i = 0; i < FileConstant.FILE_NUMS;++i) {
            File rankFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_BUYER_HASH + i);
            buyerHashFiles[i] = new RandomAccessFile(rankFile, "r");
            hashFileChannels[i] = buyerHashFiles[i].getChannel();
            hashFileMappedByteBuffers[i] = hashFileChannels[i].map(MapMode.READ_ONLY, 0, hashFileChannels[i].size());
        }
    }

    public String getBuyerLine(String buyerId) throws UnsupportedEncodingException, IOException {
        int hashFileIndex = (int) (Math.abs(buyerId.hashCode()) % FileConstant.FILE_NUMS);

        //1.查找索引
        PosInfo posInfo = null;
        if (!OneIndexCache.buyerOneIndexCache.containsKey(buyerId)) {
            return null;
        } else {
            posInfo = OneIndexCache.buyerOneIndexCache.get(buyerId);
        }
        //System.out.println(position);

        //2.按行读取内容
        MappedByteBuffer mappedByteBuffer = hashFileMappedByteBuffers[hashFileIndex];
        mappedByteBuffer.position(posInfo.offset);
        byte[] buffer = new byte[posInfo.length - 1];
        mappedByteBuffer.get(buffer);
        return new String(buffer);
//        String oneIndex = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
    }
    
    public void closeBuyerHashFiles() throws IOException {
        for (int i = 0; i < FileConstant.FILE_NUMS;++i) {
            hashFileMappedByteBuffers[i].clear();
            hashFileChannels[i].close();
            buyerHashFiles[i].close();
        }
        hashFileMappedByteBuffers = null;
        hashFileChannels = null;
        buyerHashFiles = null;
    }

}
