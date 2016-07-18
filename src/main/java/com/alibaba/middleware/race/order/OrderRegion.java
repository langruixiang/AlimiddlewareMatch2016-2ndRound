/**
 * OrderRegion.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangweiwei
 *
 */
public class OrderRegion {

    public static final int REGION_SIZE = 1000;
    
    public static final String ORDER_REGION_PREFIX = "order_region_";
    public static final String ORDER_ID_INDEX_PREFIX = ORDER_REGION_PREFIX + "orderid_index_";
    public static final int INIT_SINGLE_REGION_FILE_NUM = 10;
    
    public static final String APP_ORDER = "app_order";

    //UTF_8("UTF-8"), GB2312("GB2312"), GBK("GBK");
    public static final String ENCODING = "UTF-8";
    public static final int BYTES_OF_ORDER_ID_INDEX_FILE_LINE = 1000;//TODO

    private int regionIndex;

    private Map<String, Map<Long, String>> attributeFiles;
    
    private OrderRegion(){};

    public static OrderRegion create(int inputRegionIndex) {
        OrderRegion ret = new OrderRegion();
        ret.regionIndex = inputRegionIndex;
        ret.attributeFiles = new HashMap<String, Map<Long, String>>(OrderRegion.INIT_SINGLE_REGION_FILE_NUM);
        return ret;
    }
    
    /**
     * @param orderId
     * @param key
     * @return
     */
    public static String getAttributeDirectlyFromFile(long orderId, String key) {
        String value = null;
        long regionIndex = orderId / OrderRegion.REGION_SIZE;
        long lineIndex = orderId % OrderRegion.REGION_SIZE;
        String regionOrderIdIndexFilePath = getOrderIdIndexFilePathByRegionIndex(regionIndex);
        String line = FileUtil.getFixedBytesLine(regionOrderIdIndexFilePath, OrderRegion.ENCODING, BYTES_OF_ORDER_ID_INDEX_FILE_LINE, lineIndex, true);
        String[] splitOfLine = line.split(OrderIndexBuilder.INDEX_SPLITOR);
        String[] keysPos = splitOfLine[1].split("\\|");
        Integer keyIndex = OrderQuery.keyMap.get(key);
        
        if (keyIndex.intValue() < keysPos.length) {
            long keyPos = Integer.parseInt(keysPos[keyIndex.intValue()]);
            if (keyPos >= 0) {
                String regionKeyFilePath = getFilePathByRegionIndexAndAttributeName(regionIndex, key);
                RandomAccessFile rf = null;
                try {
                    rf = new RandomAccessFile(regionKeyFilePath, "r");
                    value = FileUtil.getLineWithRandomAccessFile(rf, OrderRegion.ENCODING, keyPos);
                    rf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        
//      long orderId = Long.parseLong(line.substring(0, firstTabIdx));//TODO remove or compare
        return value;
    }
    
    public static String getOrderIdIndexFilePathByRegionIndex(long regionIdx) {
        return OrderRegion.ORDER_ID_INDEX_PREFIX.concat(String.valueOf(regionIdx));
    }
    
    public static String getFilePathByRegionIndexAndAttributeName(long regionIdx, String attributeName) {
            return OrderRegion.ORDER_REGION_PREFIX
                    .concat(String.valueOf(regionIdx)).concat("_").concat(attributeName);
    }

    /**
     * @param orderId
     * @param key
     * @return
     */
    public String getAttributeAndCache(long orderId, String attributeName) {
        if (attributeName.startsWith(OrderRegion.APP_ORDER)) {
            Map<Long, String> attributeMap = loadAttributeFile(OrderRegion.APP_ORDER);
            String content = attributeMap.get(orderId);
            String[] appOrders = content.split("\t");
            for (int i = 0; i < appOrders.length; i++) {
                String[] keyValue = appOrders[i].split(":");
                if (attributeName.equals(keyValue[0])) {
                    return keyValue[1];
                }
            }
        } else {
            Map<Long, String> attributeMap = loadAttributeFile(attributeName);
            return attributeMap.get(orderId);
        }
        return null;
    }
    
    private Map<Long, String> loadAttributeFile(String attributeName) {
        Map<Long, String> attributeMap = attributeFiles.get(attributeName);
        if (attributeMap == null) {
            String attributeFileName = getFilePathByRegionIndexAndAttributeName(regionIndex, attributeName);
            attributeMap = loadAttributeMap(attributeFileName);
            attributeFiles.put(attributeName, attributeMap);
        }
        return attributeMap;
    }
    
    /**
     * @param regionFileName
     * @return 
     */
    private Map<Long, String> loadAttributeMap(String fileName) {
        Map<Long, String> ret = new HashMap<Long, String>(OrderRegion.REGION_SIZE, 1);
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.startsWith(String.valueOf(FileUtil.EMPTY_CHAR))) continue;
                int firstTabIdx = line.indexOf('\t');
                long orderId = Long.parseLong(line.substring(0, firstTabIdx));
                int firstEmptyCharIdx = line.indexOf(FileUtil.EMPTY_CHAR);
                if (firstEmptyCharIdx > 0) {
                    ret.put(orderId, line.substring(firstTabIdx + 1, firstEmptyCharIdx));
                } else {
                    ret.put(orderId, line.substring(firstTabIdx + 1));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public int getRegionIndex() {
        return regionIndex;
    }

}
