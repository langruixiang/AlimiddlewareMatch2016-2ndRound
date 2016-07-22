/**
 * StringIndexRegionQuery.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.middleware.race.util.FileUtil;

/**
 * @author wangweiwei
 *
 */
public class StringIndexRegionQuery {
    protected HashMap<String, Integer> keyMap;
    protected TreeMap<String, String> secondIndexMap;
    protected StringIndexCache stringIndexCache;
    protected String regionRootFolder;
    protected int regionId;
    protected String indexIdName;
    protected int maxIndexCacheSize;
    protected int cacheNumPerMiss;

    public StringIndexRegionQuery(String regionRootFolder, String indexIdName,
            int regionId, int initKeyMapCapacity, int maxIndexCacheSize, int cacheNumPerMiss) {
        this.regionRootFolder = regionRootFolder;
        this.indexIdName = indexIdName;
        this.regionId = regionId;
        stringIndexCache = new StringIndexCache(maxIndexCacheSize);
        keyMap = FileUtil.readSIHashMapFromFile(StringIndexRegion
                .getRegionKeyMapFilePath(regionRootFolder, regionId),
                initKeyMapCapacity);
        this.maxIndexCacheSize = maxIndexCacheSize;
        this.cacheNumPerMiss = cacheNumPerMiss;
    }

    public int getMaxIndexCacheSize() {
        return maxIndexCacheSize;
    }

    public void setMaxIndexCacheSize(int maxIndexCacheSize) {
        this.maxIndexCacheSize = maxIndexCacheSize;
        stringIndexCache.setMaxCacheSize(maxIndexCacheSize);
    }

    public int getCacheNumPerMiss() {
        return cacheNumPerMiss;
    }

    public void setCacheNumPerMiss(int cacheNumPerMiss) {
        this.cacheNumPerMiss = cacheNumPerMiss;
    }

    protected StringIndex getStringIndex(String indexId,
            Collection<String> filteredKeys) {
        if (!StringIndexRegion.isRegionExist(regionRootFolder, regionId)) {
            return null;
        }
        StringIndex stringIndex = null;
        stringIndex = stringIndexCache.get(indexId);
        if (stringIndex == null) {
            PosInfo indexPosInfo = getFirstIndexPosInfoFromSecondIndex(indexId);
            stringIndex = loadStringIndexFromFile(indexIdName, indexId,
                    indexPosInfo, StringIndex.ENCODING, true, filteredKeys);
            return stringIndex;
        } else if (stringIndex == StringIndex.NULL) {
            return null;
        } else {
            return loadKeyValuesOfStringIndex(stringIndex, filteredKeys);
        }
    }

    /**
     * @param indexId
     * @return
     */
    private PosInfo getFirstIndexPosInfoFromSecondIndex(String indexId) {
        if (secondIndexMap == null) {
            secondIndexMap = FileUtil.readSSTreeMapFromFile(StringIndexRegion
                    .getRegionSecondIndexFilePath(regionRootFolder, regionId,
                            indexIdName));
        }
        Map.Entry<String, String> floorEntry = secondIndexMap
                .floorEntry(indexId);
        if (floorEntry == null) {
            return null;
        }
        if (floorEntry.getKey().equals(indexId)) {
            return PosInfo.parseFromString(floorEntry.getValue());
        } else {
            Map.Entry<String, String> ceilEntry = secondIndexMap
                    .ceilingEntry(indexId);
            if (ceilEntry == null) {
                return null;
            }
            String floorValue = floorEntry.getValue();
            PosInfo floorPosInfo = PosInfo
                    .parseFromString(floorValue.substring(floorValue
                            .indexOf(StringIndex.INDEX_SPLITOR) + 1));
            String ceilValue = ceilEntry.getValue();
            PosInfo ceilPosInfo = PosInfo
                    .parseFromString(ceilValue.substring(ceilValue
                            .indexOf(StringIndex.INDEX_SPLITOR) + 1));
            int offset = floorPosInfo.offset + floorPosInfo.length;
            int length = ceilPosInfo.offset - offset;
            return new PosInfo(offset, length);
        }
    }

    protected StringIndex loadStringIndexFromFile(String indexIdName,
            String indexId, PosInfo indexPosInfo, String encoding,
            boolean cacheMoreIndex, Collection<String> keys) {// TODO
        String regionIndexFile = StringIndexRegion.getRegionFirstIndexFilePath(
                regionRootFolder, regionId, indexIdName);
        RandomAccessFile rf = null;
        StringIndex ret = null;
        try {
            rf = new RandomAccessFile(regionIndexFile, "r");
            // FileChannel fc = rf.getChannel();
            // MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
            // fc.size());
            rf.seek(indexPosInfo.offset);
            int lengthCount = 0;
            String line = null;
            int cacheCount = cacheMoreIndex ? cacheNumPerMiss : 0;
            while ((line = rf.readLine()) != null) {
                if (ret == null && lengthCount < indexPosInfo.length) {
                    lengthCount += line.concat("\n").getBytes(encoding).length;
                    line = line.trim();
                    int indexSplitorIndex = line
                            .indexOf(StringIndex.INDEX_SPLITOR);
                    String indexIdInLine = line.substring(0, indexSplitorIndex);
                    int compareResult = indexIdInLine.compareTo(indexId);
                    if (compareResult == 0) {
                        ret = StringIndex.parseFromLine(line);
                        ret = loadKeyValuesOfStringIndex(ret, keys);
                        stringIndexCache.put(ret.getIndexId(), ret);
                    }
                } else if (cacheCount > 0) {
                    --cacheCount;
                    StringIndex stringIndex = StringIndex.parseFromLine(line);
                    if (stringIndexCache.contains(indexId)) {
                        break;
                    }
                    stringIndex = loadKeyValuesOfStringIndex(stringIndex, keys);
                    stringIndexCache.put(stringIndex.getIndexId(), stringIndex);
                } else {
                    break;
                }
            }
            // fc.close();
            rf.close();
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    protected StringIndex loadKeyValuesOfStringIndex(StringIndex stringIndex,
            Collection<String> keys) {
        if (stringIndex == StringIndex.NULL) {
            return stringIndex;
        }
        LinkedList<String> needLoadKeys = stringIndex.getNeedLoadKeys(keys);
        return loadKeyValuesFromFile(stringIndex, needLoadKeys);
    }

    protected StringIndex loadKeyValuesFromFile(StringIndex stringIndex,
            LinkedList<String> needLoadKeys) {
        String regionKeyValuesFile = StringIndexRegion
                .getRegionKeyValuesFilePath(regionRootFolder, regionId);
        RandomAccessFile rf = null;
        try {
            rf = new RandomAccessFile(regionKeyValuesFile, "r");
            FileChannel fc = rf.getChannel();
            long fileSize = fc.size();
            MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileSize);
            for (String key : needLoadKeys) {
                if (key.equals(stringIndex.getIndexIdName())) {
                    stringIndex.addKeyValue(key,
                            String.valueOf(stringIndex.getIndexId()));
                } else {
                    Integer keyIndex = this.keyMap.get(key);
                    stringIndex.loadKeyValue(keyIndex, key, mbb);
                }
            }
            fc.close();
            rf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stringIndex;
    }
}
