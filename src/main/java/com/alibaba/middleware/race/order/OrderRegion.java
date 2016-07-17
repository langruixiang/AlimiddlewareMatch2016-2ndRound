/**
 * OrderRegion.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangweiwei
 *
 */
public class OrderRegion {

    private int regionIndex;

    private Map<String, Map<Long, String>> attributeFiles;
    
    private OrderRegion(){};

    public static OrderRegion create(int inputRegionIndex) {
        OrderRegion ret = new OrderRegion();
        ret.regionIndex = inputRegionIndex;
        ret.attributeFiles = new HashMap<String, Map<Long, String>>(OrderIndexBuilder.INIT_SINGLE_REGION_FILE_NUM);
        return ret;
    }

    /**
     * @param orderId
     * @param key
     * @return
     */
    public String getAttribute(long orderId, String attributeName) {
        if (attributeName.startsWith(OrderIndexBuilder.APP_ORDER)) {
            Map<Long, String> attributeMap = loadAttributeFile(OrderIndexBuilder.APP_ORDER);
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
    
    public Map<Long, String> loadAttributeFile(String attributeName) {
        Map<Long, String> attributeMap = attributeFiles.get(attributeName);
        if (attributeMap == null) {
            String attributeFileName = OrderIndexBuilder.ORDER_REGION_PREFIX
                    .concat(String.valueOf(regionIndex)).concat("_").concat(attributeName);
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
        Map<Long, String> ret = new HashMap<Long, String>(OrderIndexBuilder.REGION_SIZE, 1);
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String line = null;
            while ((line = br.readLine()) != null) {
                int firstTabIdx = line.indexOf('\t');
                long orderId = Long.parseLong(line.substring(0, firstTabIdx));
                String content = line.substring(firstTabIdx + 1);
                ret.put(orderId, content);
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
