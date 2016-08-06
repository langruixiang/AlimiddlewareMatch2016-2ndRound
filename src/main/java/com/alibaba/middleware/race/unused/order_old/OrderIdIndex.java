package com.alibaba.middleware.race.unused.order_old;
//package com.alibaba.middleware.race.order_old;
//
//import java.io.RandomAccessFile;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.Map;
//
//public class OrderIdIndex {
//
//    private long id;
//    
//    private String[] keysPos;
//    
//    private long regionIndex;
//    
//    public static OrderIdIndex NULL = new OrderIdIndex();
//
//    private Map<String, String> keyValueMap = null;
//    
//    public long getId() {
//        return id;
//    }
//
//    public void setId(long id) {
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
//    public long getRegionIndex() {
//        return regionIndex;
//    }
//
//    public void setRegionIndex(long regionIndex) {
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
//    public static OrderIdIndex parseFromLine(String line) {
//        if (line == null || line.isEmpty()) {
//            return OrderIdIndex.NULL;
//        }
//        OrderIdIndex idIndex = new OrderIdIndex();
//        String[] splitOfLine = line.split(OrderIndexBuilder.INDEX_SPLITOR);
//        long parsedId = Long.parseLong(splitOfLine[0]);
//        idIndex.setId(parsedId);
//        idIndex.setRegionIndex(parsedId / OrderRegion.REGION_SIZE);
//        String[] parsedKeysPos = splitOfLine[1].split("\\|");
//        idIndex.setKeysPos(parsedKeysPos);
//        return idIndex;
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
//        if (keyValueMap == null) {
//            keyValueMap = new HashMap<String, String>();
//        }
//        if (keyIndex < keysPos.length) {
//            long keyPos = Integer.parseInt(keysPos[keyIndex]);
//            if (keyPos >= 0) {
//                String value = FileUtil.getLineWithRandomAccessFile(file, OrderRegion.ENCODING, keyPos);
//                keyValueMap.put(key, value);
//            } else {
//                keyValueMap.put(key, null);
//            }
//        } else {
//            keyValueMap.put(key, null);
//        }
//    }
//
//    /**
//     * @param keys
//     * @return
//     */
//    public LinkedList<String> getNeedLoadKeys(Collection<String> keys) {
//        if (keyValueMap == null) {
//            keyValueMap = new HashMap<String, String>();
//        }
//        LinkedList<String> ret= new LinkedList<String>();
//        for (String key : keys) {
//            if (!keyValueMap.containsKey(key)) {
//                ret.add(key);
//            }
//        }
//        return ret;
//    }
//    
//    public void addKeyValue(String key, String value) {
//        if (keyValueMap == null) {
//            keyValueMap = new HashMap<String, String>();
//        }
//        
//        keyValueMap.put(key, value);
//    }
//}
