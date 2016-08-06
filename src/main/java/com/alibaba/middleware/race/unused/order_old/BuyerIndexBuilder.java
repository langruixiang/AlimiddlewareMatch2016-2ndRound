package com.alibaba.middleware.race.unused.order_old;
//package com.alibaba.middleware.race.order;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.OutputStreamWriter;
//import java.io.RandomAccessFile;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
///**
// * @author wangweiwei
// *
// */
//public class BuyerIndexBuilder {
//    public static final String ID_INDEX_DIR = "buyer_id_index";
//    public static final String KEY_MAP_FILE = ID_INDEX_DIR + "/" + "buyer_key_map.txt";
//    public static final int INIT_KEY_MAP_CAPACITY = 20;
//    public static final String INDEX_SPLITOR = ":";
//    public static final String KEY_SPLITOR = "|";
//    public static final Map<String, Integer> keyMap = new HashMap<String, Integer>(INIT_KEY_MAP_CAPACITY);
//    
//    public static Map<String, RandomAccessFile> singleRegionFilesMap = new HashMap<String, RandomAccessFile>(BuyerRegion.INIT_SINGLE_REGION_FILE_NUM);
//    public static long curRegionIndex = -1;
//
//    public static void build(Collection<String> buyerFiles) {
//        try {
//            FileUtil.createDir(BuyerIndexBuilder.ID_INDEX_DIR);
//            for (String file : buyerFiles) {
//                BufferedReader br = new BufferedReader(new FileReader(file));
//
//                String line = null;
//                while ((line = br.readLine()) != null) {
//                    buildWithLine(line);
//                }
//                br.close();
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
//    private static void writeKeyMapFile(Map<String, Integer> map) {
//        try {
//            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(KEY_MAP_FILE), BuyerRegion.ENCODING));
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
//    private static void buildWithLine(String line) throws IOException {
//        BuyerIdIndex buyer = new BuyerIdIndex();
//        String[] keyValues = line.split("\t");
//        for (int i = 0; i < keyValues.length; i++) {
//            String[] keyValue = keyValues[i].split(":");
//            if ("buyerid".equals(keyValue[0])) {
//                buyer.setId(keyValue[1]);
//                buyer.setRegionIndex(BuyerIdIndex.getRegionIndexById(keyValue[1]));
//            } else {
//                buyer.addKeyValue(keyValue[0], keyValue[1]);
//                if(!keyMap.containsKey(keyValue[0])) {
//                    keyMap.put(keyValue[0], keyMap.size());
//                }
//            }
//        }
//
//        // write region key file
//        String regionFileName = null;
//        Map<String, String> keyValueMap = buyer.getKeyValueMap();
//        ArrayList<Long> keyPosInfo = new ArrayList<Long>(keyMap.size());
//        for (int i = 0; i < keyMap.size(); ++i) {
//            keyPosInfo.add(-1L);
//        }
//        for (Entry<String, String> entry : keyValueMap.entrySet()) {
//            Integer keyIndex = keyMap.get(entry.getKey());
//            regionFileName = BuyerRegion.getFilePathByRegionIndexAndKey(buyer.getRegionIndex(), entry.getKey());
//            long startPos = writeLineToRegionKeyFile(regionFileName, buyer.getRegionIndex(), buyer.getId(), entry.getValue());
//            keyPosInfo.set(keyIndex, startPos);
//        }
//        
//        // write idIndexFile
//        String idIndexFileName = BuyerRegion.getIdIndexFilePathByRegionIndex(buyer.getRegionIndex());
//        StringBuilder indexLineSb = new StringBuilder();
//        indexLineSb.append(buyer.getId()).append(INDEX_SPLITOR).append(keyPosInfo.get(0));
//        for(int i = 1; i < keyPosInfo.size(); ++i) {
//            indexLineSb.append(KEY_SPLITOR).append(keyPosInfo.get(i));
//        }
//        writeLineToRegionIndexFile(idIndexFileName, buyer.getRegionIndex(), buyer.getId(), indexLineSb.toString());
//    }
//
//    /**
//     * 
//     */
//    private static void writeLineToRegionIndexFile(String regionFileName, long regionIndex,
//            long buyerId, String content) throws IOException {
//        if (regionIndex != curRegionIndex) {
//            for (Entry<String, RandomAccessFile> entry : singleRegionFilesMap.entrySet()) {
//                entry.getValue().close();
//            }
//            singleRegionFilesMap.clear();
//            curRegionIndex = regionIndex;
//        }
//        long lineIndex = buyerId % BuyerRegion.REGION_SIZE;
//        RandomAccessFile regionFile = singleRegionFilesMap.get(regionFileName);
//        if (regionFile == null) {
//            regionFile = new RandomAccessFile(regionFileName, "rw");
//            singleRegionFilesMap.put(regionFileName, regionFile);
//        }
//        FileUtil.writeFixedBytesLineWithFile(regionFile, BuyerRegion.ENCODING, content,
//                BuyerRegion.BYTES_OF_BUYER_ID_INDEX_FILE_LINE, lineIndex);
//    }
//
//    private static long writeLineToRegionKeyFile(String regionFileName, long regionIndex,
//            long buyerId, String content) throws IOException {
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
//        return FileUtil.appendLineWithRandomAccessFile(regionFile, BuyerRegion.ENCODING, content);
//    }
//}
