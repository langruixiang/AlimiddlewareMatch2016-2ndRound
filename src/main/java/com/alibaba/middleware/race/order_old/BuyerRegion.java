//package com.alibaba.middleware.race.order;
//
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.util.Collection;
//import java.util.HashMap;
///**
// * @author wangweiwei
// *
// */
//public class BuyerRegion {
//
//    public static final int REGION_SIZE = 10000;
//    
//    public static final String REGION_PREFIX = BuyerIndexBuilder.ID_INDEX_DIR + "/" + "buyer_region_";
//    public static final String ID_INDEX_PREFIX = REGION_PREFIX + "buyerid_index_";
//    public static final int INIT_SINGLE_REGION_FILE_NUM = 10;
//    
//    //UTF_8("UTF-8"), GB2312("GB2312"), GBK("GBK");
//    public static final String ENCODING = "UTF-8";
//    public static final int BYTES_OF_BUYER_ID_INDEX_FILE_LINE = 1000;//TODO
//
//    private int regionIndex;
//    
//    public static HashMap<String, Integer> keyMap;
//    static {
//        keyMap = FileUtil.readSIHashMapFromFile(BuyerIndexBuilder.KEY_MAP_FILE, BuyerIndexBuilder.INIT_KEY_MAP_CAPACITY);
//    }
//
//    private BuyerRegion(){};
//
//    public static BuyerRegion create(int inputRegionIndex) {
//        BuyerRegion ret = new BuyerRegion();
//        ret.regionIndex = inputRegionIndex;
//        return ret;
//    }
//    
//    public static String getIdIndexFilePathByRegionIndex(String regionIdx) {
//        return BuyerRegion.ID_INDEX_PREFIX.concat(regionIdx);
//    }
//    
//    public static String getFilePathByRegionIndexAndKey(String regionIdx, String key) {
//            return BuyerRegion.REGION_PREFIX
//                    .concat(regionIdx).concat("_").concat(key);
//    }
//
//    public BuyerIdIndex getBuyerIdIndex(long buyerId, Collection<String> keys) {
//        BuyerIdIndex buyerIdIndex = loadBuyerIdIndexFromFile(buyerId);
//        buyerIdIndex = loadKeyValuesOfBuyerIdIndex(buyerIdIndex, keys);
//        return buyerIdIndex;
//    }
//
//    private BuyerIdIndex loadBuyerIdIndexFromFile(long buyerId) {
//        long regionIndex = buyerId / BuyerRegion.REGION_SIZE;
//        long lineIndex = buyerId % BuyerRegion.REGION_SIZE;
//        String regionBuyerIdIndexFilePath = BuyerRegion.getIdIndexFilePathByRegionIndex(regionIndex);
//        String line = FileUtil.getFixedBytesLine(regionBuyerIdIndexFilePath, BuyerRegion.ENCODING, BuyerRegion.BYTES_OF_BUYER_ID_INDEX_FILE_LINE, lineIndex, true);
//        BuyerIdIndex buyerIdIndex = BuyerIdIndex.parseFromLine(line);
//        return buyerIdIndex;
//    }
//
//
//    /**
//     * @param BuyerIdIndex
//     * @param keys
//     */
//    private BuyerIdIndex loadKeyValuesOfBuyerIdIndex(BuyerIdIndex buyerIdIndex, Collection<String> keys) {
//        if (buyerIdIndex == BuyerIdIndex.NULL) {
//            return buyerIdIndex;
//        }
//        for (String key : keys) {
//            String regionKeyFilePath = getFilePathByRegionIndexAndKey(regionIndex, key);
//            RandomAccessFile rf = null;
//            try {
//                rf = new RandomAccessFile(regionKeyFilePath, "r");
//                Integer keyIndex = keyMap.get(key);
//                buyerIdIndex.loadKeyValue(keyIndex, key, rf);
//                rf.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        return buyerIdIndex;
//    }
//
//    /**
//     * @return
//     */
//    public static boolean isRegionExist(long regionIdx) {
//        return FileUtil.isFileExist(getIdIndexFilePathByRegionIndex(regionIdx));
//    }
//
//}
