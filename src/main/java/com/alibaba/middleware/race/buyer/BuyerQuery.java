package com.alibaba.middleware.race.buyer;

import com.alibaba.middleware.race.cache.FileNameCache;
import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.cache.RandomFile;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.FilePosition;
import com.alibaba.middleware.race.model.Order;
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
public class BuyerQuery {
    public static Buyer findBuyerById(String buyerId, int index) {
        if (buyerId == null || buyerId.isEmpty()) return null;
        Buyer buyer = new Buyer();
        try {

            String str = null;

            //1.查找索引
            FilePosition positionInfo = null;
            if (!OneIndexCache.buyerOneIndexCache.containsKey(buyerId)) {
                return null;
            } else {
                positionInfo = OneIndexCache.buyerOneIndexCache.get(buyerId);
            }

            File rankFile = new File(FileNameCache.fileNameMap.get(positionInfo.getFileNum()));
            RandomAccessFile hashRaf = new RandomAccessFile(rankFile, "r");

            //RandomAccessFile hashRaf = RandomFile.randomFileMap.get(positionInfo.getFileName());

            //2.按行读取内容
            hashRaf.seek(positionInfo.getPosition());
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

    public static void main(String args[]) {

        //BuyerIdIndexFile.generateBuyerIdIndex();
        //findByBuyerId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
