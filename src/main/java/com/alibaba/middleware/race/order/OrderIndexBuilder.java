/**
 * OrderIndexBuilder.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.order;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author wangweiwei
 *
 */
public class OrderIndexBuilder {

    public static final int REGION_SIZE = 1000;
    
    public static final int INIT_SINGLE_REGION_FILE_NUM = 10;
    
    public static final String ORDER_REGION_PREFIX = "order_region_";
    
    public static final String APP_ORDER = "app_order";
    
    public static final long INVALID_ORDER_ID = -1;
    
    public static Map<String, BufferedWriter> singleRegionWritersMap = new HashMap<String, BufferedWriter>(INIT_SINGLE_REGION_FILE_NUM);
    public static int curRegionIndex = -1;

    public static void build(Collection<String> orderFiles) {
        try {
            for (String orderFile : orderFiles) {
                BufferedReader order_br = new BufferedReader(new FileReader(orderFile));

                String line = null;
                while ((line = order_br.readLine()) != null) {
                    long orderId = INVALID_ORDER_ID;
                    int regionIndex = -1;
                    String regionFileName = null;
                    String appOrderInfo = null;
                    String[] keyValues = line.split("\t");
                    for (int i = 0; i < keyValues.length; i++) {
                        String[] keyValue = keyValues[i].split(":");
                        if ("orderid".equals(keyValue[0])) {
                            orderId = Long.parseLong(keyValue[1]);
                            regionIndex = (int) (orderId / REGION_SIZE);
                        } else if (keyValue[0].startsWith(APP_ORDER)){
                            if (appOrderInfo == null) {
                                appOrderInfo = keyValues[i];
                            } else {
                                appOrderInfo = appOrderInfo.concat("\t").concat(keyValues[i]);
                            }
                        } else {
                            //TODO make sure regionIndex is valid
                            regionFileName = ORDER_REGION_PREFIX.concat(String.valueOf(regionIndex)).concat("_").concat(keyValue[0]);
                            appendContentToRegionFile(regionFileName, regionIndex, orderId, keyValue[1]);
                        }
                    }
                    // write appOrderInfo into file
                    if (appOrderInfo != null) {
                        //TODO make sure regionIndex is valid
                        regionFileName = ORDER_REGION_PREFIX.concat(String.valueOf(regionIndex)).concat("_").concat(APP_ORDER);
                        appendContentToRegionFile(regionFileName, regionIndex, orderId, appOrderInfo);
                    }
                }
                order_br.close();
            }
            for (Entry<String, BufferedWriter> entry : singleRegionWritersMap.entrySet()) {
                entry.getValue().close();
            }
            singleRegionWritersMap.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param regionFileName
     * @param orderId
     * @param string
     * @throws IOException 
     */
    private static void appendContentToRegionFile(String regionFileName, int regionIndex,
            long orderId, String content) throws IOException {
        if (regionIndex != curRegionIndex) {
            for (Entry<String, BufferedWriter> entry : singleRegionWritersMap.entrySet()) {
                entry.getValue().close();
            }
            singleRegionWritersMap.clear();
            curRegionIndex = regionIndex;
        }
        BufferedWriter writer = singleRegionWritersMap.get(regionFileName);
        if (writer == null) {
            writer = new BufferedWriter(new FileWriter(regionFileName));
            singleRegionWritersMap.put(regionFileName, writer);
        }
        writer.write(String.valueOf(orderId).concat("\t").concat(content));
        writer.newLine();
    }
}
