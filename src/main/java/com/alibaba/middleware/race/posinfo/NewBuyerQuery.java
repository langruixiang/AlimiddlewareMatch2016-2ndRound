package com.alibaba.middleware.race.posinfo;

import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.model.PosInfo;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class NewBuyerQuery {
    public static Buyer findBuyerById(String buyerId, int index) {
        if (buyerId == null || buyerId.isEmpty()) return null;
        Buyer buyer = new Buyer();
        try {
            //1.查找索引
            PosInfo posInfo = NewOneIndexCache.buyerOneIndexCache.get(buyerId);
            if (posInfo == null) {
                return null;
            }

            File rankFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_BUYER_HASH + index);
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "r");
            //2.按行读取内容
            hashRaf.seek(posInfo.offset);
            byte[] buffer = new byte[posInfo.length];
            hashRaf.read(buffer);
            String oneIndex = new String(buffer);

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

    public static void main(String args[]) {

        //BuyerIdIndexFile.generateBuyerIdIndex();
        //findByBuyerId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
