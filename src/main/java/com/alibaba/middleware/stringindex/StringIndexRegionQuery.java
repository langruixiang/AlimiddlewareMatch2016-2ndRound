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

import com.alibaba.middleware.race.util.FileUtil;

/**
 * @author wangweiwei
 *
 */
public class StringIndexRegionQuery {
    public static final int DEFAULT_INIT_KEY_MAP_CAPACITY = 20;
    public static final int DEFAULT_MAX_INDEX_CACHE_SIZE = 1000;
    public static final int DEFAULT_CACHE_NUM_PER_MISS = 100;

    protected HashMap<String, Integer> keyMap;
    protected StringIndexCache stringIndexCache;
    protected String regionRootFolder;
    protected int regionId;
    protected String indexIdName;
    
    protected int maxIndexCacheSize = DEFAULT_MAX_INDEX_CACHE_SIZE;
    protected int cacheNumPerMiss = DEFAULT_CACHE_NUM_PER_MISS;

    public StringIndexRegionQuery (String regionRootFolder, String indexIdName, int regionId, int initKeyMapCapacity) {
        this.regionRootFolder = regionRootFolder;
        this.indexIdName = indexIdName;
        this.regionId = regionId;
        stringIndexCache = new StringIndexCache(DEFAULT_MAX_INDEX_CACHE_SIZE);
        keyMap = FileUtil.readSIHashMapFromFile(
                StringIndexRegion.getRegionKeyMapFilePath(regionRootFolder, regionId),
                initKeyMapCapacity);
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

    protected StringIndex getStringIndex(String indexId, Collection<String> filteredKeys) {
        if (!StringIndexRegion.isRegionExist(regionRootFolder, regionId)) {
            return null;
        }
        StringIndex stringIndex = null;
        stringIndex = stringIndexCache.get(indexId);
        if (stringIndex == null) {
            stringIndex = loadStringIndexFromFile(indexIdName, indexId,
                    StringIndex.BYTES_OF_INDEX_FILE_LINE, StringIndex.ENCODING,
                    true, filteredKeys);
            return stringIndex;
        } else if (stringIndex == StringIndex.NULL) {
            return null;
        } else {
            return loadKeyValuesOfStringIndex(stringIndex, filteredKeys);
        }
    }
    

    protected StringIndex loadStringIndexFromFile(String indexIdName, String indexId,
            int bytesOfLine, String encoding, boolean cacheMoreIndex,
            Collection<String> keys) {
        String regionIndexFile = StringIndexRegion.getRegionIndexFilePath(regionRootFolder, regionId, indexIdName);
        RandomAccessFile rf = null;
        StringIndex ret = null;
        try {
            rf = new RandomAccessFile(regionIndexFile, "r");
            FileChannel fc = rf.getChannel();
            long fileSize = fc.size();
            MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileSize);
            byte[] buffer = new byte[bytesOfLine];
            String line = null;
            int cacheCount = cacheMoreIndex ? cacheNumPerMiss : 0;
            while (mbb.hasRemaining()) {
                if (ret == null) {
                    mbb.get(buffer);
                    line = new String(buffer, encoding).trim();
                    int indexSplitorIndex = line.indexOf(StringIndex.INDEX_SPLITOR);
                    String indexIdInLine = line.substring(0, indexSplitorIndex);
                    int compareResult = indexIdInLine.compareTo(indexId);
                    if ( compareResult > 0) {
                        break;
                    } else if (compareResult == 0) {
                        ret = StringIndex.parseFromLine(line);
                        ret = loadKeyValuesOfStringIndex(ret, keys);
                        stringIndexCache.put(indexId, ret);
                    }
                } else if (cacheCount > 0) {
                    --cacheCount;
                    mbb.get(buffer);
                    line = new String(buffer, encoding);
                    StringIndex stringIndex = StringIndex.parseFromLine(line);
                    if (stringIndexCache.contains(indexId)) {
                        break;
                    }
                    stringIndex = loadKeyValuesOfStringIndex(stringIndex, keys);
                    stringIndexCache.put(indexId, stringIndex);
                } else {
                    break;
                }
            }
            fc.close();
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
        String regionKeyValuesFile = StringIndexRegion.getRegionKeyValuesFilePath(regionRootFolder, regionId);
        RandomAccessFile rf = null;
        try {
            rf = new RandomAccessFile(regionKeyValuesFile, "r");
            FileChannel fc = rf.getChannel();
            long fileSize = fc.size();
            MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileSize);
            for (String key : needLoadKeys) {
                if (key.equals(stringIndex.getIndexIdName())) {
                    stringIndex.addKeyValue(key, String.valueOf(stringIndex.getIndexId()));
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
