package com.alibaba.middleware.race.unused.order_old;
///**
// * OrderIndexBuilder.java
// * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
// * any form of usage is subject to approval.
// */
//package com.alibaba.middleware.race.order_old;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.FileOutputStream;
//import java.io.FileReader;
//import java.io.IOException;
//import java.io.OutputStreamWriter;
//import java.io.RandomAccessFile;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.concurrent.CountDownLatch;
//
//import com.alibaba.middleware.race.constant.FileConstant;
//
///**
// * @author wangweiwei
// *
// */
//public class OrderIndexBuilder extends Thread {
//    public static String ORDER_ID_INDEX_DIR;
//    public static String ORDER_KEY_MAP_FILE = ORDER_ID_INDEX_DIR + "/" + "order_key_map.txt";
//    public static final int INIT_KEY_MAP_CAPACITY = 20;
//    public static final String INDEX_SPLITOR = ":";
//    public static final String KEY_SPLITOR = "|";
//    public static final Map<String, Integer> keyMap = new HashMap<String, Integer>(INIT_KEY_MAP_CAPACITY);
//    
//    public static Map<String, RandomAccessFile> singleRegionFilesMap = new HashMap<String, RandomAccessFile>(OrderRegion.INIT_SINGLE_REGION_FILE_NUM);
//    public static long curRegionIndex = -1;
//    
//    private Collection<String> orderFiles;
//    private Collection<String> storeFolders;
//    private CountDownLatch countDownLatch;
//    
//    public OrderIndexBuilder(Collection<String> orderFiles, Collection<String> storeFolders, CountDownLatch countDownLatch) {
//        this.orderFiles = orderFiles;
//        this.storeFolders = storeFolders;
//        this.countDownLatch = countDownLatch;
//        if (FileConstant.THIRD_DISK_PATH.endsWith("/")) {
//            ORDER_ID_INDEX_DIR = FileConstant.THIRD_DISK_PATH + "order_id_index";
//        } else {
//            ORDER_ID_INDEX_DIR = FileConstant.THIRD_DISK_PATH + "/" + "order_id_index";
//        }
//        ORDER_KEY_MAP_FILE = ORDER_ID_INDEX_DIR + "/" + "order_key_map.txt";
//        
//    }
//
//    public void build() {
//        try {
//            FileUtil.createDir(OrderIndexBuilder.ORDER_ID_INDEX_DIR);
//            for (String orderFile : orderFiles) {
//                BufferedReader order_br = new BufferedReader(new FileReader(orderFile));
//
//                String line = null;
//                while ((line = order_br.readLine()) != null) {
//                    buildWithOrderLine(line);
//                }
//                order_br.close();
//            }
//            writeKeyMapFile(keyMap);
//            for (Entry<String, RandomAccessFile> entry : singleRegionFilesMap.entrySet()) {
//                entry.getValue().close();
//            }
//            singleRegionFilesMap.clear();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 
//     */
//    private void writeKeyMapFile(Map<String, Integer> map) {
//        try {
//            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(ORDER_KEY_MAP_FILE), OrderRegion.ENCODING));
//            for (Entry<String, Integer> entry : keyMap.entrySet()) {
//                writer.write(entry.getKey().concat(":").concat(entry.getValue().toString()).concat("\n"));
//            }
//            writer.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        
//    }
//
//    /**
//     * @throws IOException 
//     * 
//     */
//    private void buildWithOrderLine(String orderLine) throws IOException {
//        OrderIdIndex order = new OrderIdIndex();
//        String[] keyValues = orderLine.split("\t");
//        for (int i = 0; i < keyValues.length; i++) {
//            String[] keyValue = keyValues[i].split(":");
//            if ("orderid".equals(keyValue[0])) {
//                order.setId(Long.parseLong(keyValue[1]));
//                order.setRegionIndex(order.getId() / OrderRegion.REGION_SIZE);
//            } else {
//                order.addKeyValue(keyValue[0], keyValue[1]);
//                if(!keyMap.containsKey(keyValue[0])) {
//                    keyMap.put(keyValue[0], keyMap.size());
//                }
//            }
//        }
//
//        // write region key file
//        String regionFileName = null;
//        Map<String, String> keyValueMap = order.getKeyValueMap();
//        ArrayList<Long> keyPosInfo = new ArrayList<Long>(keyMap.size());
//        for (int i = 0; i < keyMap.size(); ++i) {
//            keyPosInfo.add(-1L);
//        }
//        for (Entry<String, String> entry : keyValueMap.entrySet()) {
//            Integer keyIndex = keyMap.get(entry.getKey());
//            regionFileName = OrderRegion.getFilePathByRegionIndexAndKey(order.getRegionIndex(), entry.getKey());
//            long startPos = writeLineToRegionKeyFile(regionFileName, order.getRegionIndex(), order.getId(), entry.getValue());
//            keyPosInfo.set(keyIndex, startPos);
//        }
//        
//        // write orderIdIndexFile
//        String orderIdIndexFileName = OrderRegion.getOrderIdIndexFilePathByRegionIndex(order.getRegionIndex());
//        StringBuilder indexLineSb = new StringBuilder();
//        indexLineSb.append(order.getId()).append(INDEX_SPLITOR).append(keyPosInfo.get(0));
//        for(int i = 1; i < keyPosInfo.size(); ++i) {
//            indexLineSb.append(KEY_SPLITOR).append(keyPosInfo.get(i));
//        }
//        writeLineToRegionIndexFile(orderIdIndexFileName, order.getRegionIndex(), order.getId(), indexLineSb.toString());
//    }
//
//    /**
//     * 
//     */
//    private void writeLineToRegionIndexFile(String regionFileName, long regionIndex,
//            long orderId, String content) throws IOException {
//        if (regionIndex != curRegionIndex) {
//            for (Entry<String, RandomAccessFile> entry : singleRegionFilesMap.entrySet()) {
//                entry.getValue().close();
//            }
//            singleRegionFilesMap.clear();
//            curRegionIndex = regionIndex;
//        }
//        long lineIndex = orderId % OrderRegion.REGION_SIZE;
//        RandomAccessFile regionFile = singleRegionFilesMap.get(regionFileName);
//        if (regionFile == null) {
//            regionFile = new RandomAccessFile(regionFileName, "rw");
//            singleRegionFilesMap.put(regionFileName, regionFile);
//        }
//        FileUtil.writeFixedBytesLineWithFile(regionFile, OrderRegion.ENCODING, content,
//                OrderRegion.BYTES_OF_ORDER_ID_INDEX_FILE_LINE, lineIndex);
//    }
//
//    /**
//     * @param regionFileName
//     * @param orderId
//     * @param string
//     * @throws IOException 
//     */
//    private long writeLineToRegionKeyFile(String regionFileName, long regionIndex,
//            long orderId, String content) throws IOException {
//        if (regionIndex != curRegionIndex) {
//            for (Entry<String, RandomAccessFile> entry : singleRegionFilesMap.entrySet()) {
//                entry.getValue().close();
//            }
//            singleRegionFilesMap.clear();
//            curRegionIndex = regionIndex;
//        }
//        RandomAccessFile regionFile = singleRegionFilesMap.get(regionFileName);
//        if (regionFile == null) {
//            regionFile = new RandomAccessFile(regionFileName, "rw");
//            singleRegionFilesMap.put(regionFileName, regionFile);
//        }
//        return FileUtil.appendLineWithRandomAccessFile(regionFile, OrderRegion.ENCODING, content);
////        return FileUtil.appendLineWithRandomAccessFile(regionFile, OrderRegion.ENCODING, String.valueOf(orderId).concat(":") + content);
//    }
//    
//    @Override
//    public void run(){
//        build();
//        System.out.println("OrderIndexBuilder build end~");
//        countDownLatch.countDown();
//    }
//}
