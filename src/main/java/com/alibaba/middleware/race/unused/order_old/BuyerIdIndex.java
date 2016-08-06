package com.alibaba.middleware.race.unused.order_old;
//package com.alibaba.middleware.race.order;
//
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.Map;
//import java.util.Map.Entry;
//
//public class BuyerIdIndex {
//
//    private String id;
//    
//    private String[] keysPos;
//    
//    private String regionIndex;
//    
//    public static BuyerIdIndex NULL = new BuyerIdIndex();
//
//    private Map<String, String> keyValueMap = null;
//    
//    public String getId() {
//        return id;
//    }
//
//    public void setId(String id) {
//        this.id = id;
//    }
//
//    public void setKeysPos(String[] keyPos) {
//        this.keysPos = keyPos;
//    }
//
//    public String[] getKeysPos() {
//        return keysPos;
//    }
//
//    public String getRegionIndex() {
//        return regionIndex;
//    }
//
//    public void setRegionIndex(String regionIndex) {
//        this.regionIndex = regionIndex;
//    }
//
//    public Map<String, String> getKeyValueMap() {
//        return keyValueMap;
//    }
//
//    public void setKeyValueMap(Map<String, String> keyValueMap) {
//        this.keyValueMap = keyValueMap;
//    }
//
//    public static BuyerIdIndex parseFromLine(String line) {
//        if (line == null || line.isEmpty()) {
//            return BuyerIdIndex.NULL;
//        }
//        BuyerIdIndex idIndex = new BuyerIdIndex();
//        String[] splitOfLine = line.split(BuyerIndexBuilder.INDEX_SPLITOR);
//        idIndex.setId(splitOfLine[0]);
//        idIndex.setRegionIndex(getRegionIndexById(splitOfLine[0]));
//        String[] parsedKeysPos = splitOfLine[1].split("\\|");
//        idIndex.setKeysPos(parsedKeysPos);
//        idIndex.setKeyValueMap(new HashMap<String, String>());
//        return idIndex;
//    }
//    
//    public static String getRegionIndexById(String inputId) {
//        return inputId.substring(inputId.length() - 1);
//    }
//
//    public String getValueByKey(String key) {
//        if (keyValueMap == null) return null;
//        return keyValueMap.get(key);
//    }
//
//    /**
//     * @param keys
//     */
//    public void loadKeyValue(int keyIndex, String key, RandomAccessFile file) {
//        if (keyIndex < keysPos.length) {
//            long keyPos = Integer.parseInt(keysPos[keyIndex]);
//            if (keyPos >= 0) {
//                String value = FileUtil.getLineWithRandomAccessFile(file, BuyerRegion.ENCODING, keyPos);
//                keyValueMap.put(key, value);
//            } else {
//                keyValueMap.put(key, null);
//            }
//        } else {
//            keyValueMap.put(key, null);
//        }
//    }
//
//    public void addKeyValue(String key, String value) {
//        keyValueMap.put(key, value);
//    }
//}
