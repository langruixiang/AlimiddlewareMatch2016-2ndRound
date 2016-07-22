/**
 * StringIndexRegion.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import com.alibaba.middleware.race.util.FileUtil;

/**
 * @author wangweiwei
 *
 */
public class StringIndexRegion {

    public static final String REGION_PREFIX = "region_";
    public static final String REGION_INDEX_FILE_PREFIX = "regionIndex_";
    public static final String REGION_KEY_MAP_FILE_NAME = "regionKeyMap.txt";
    public static final String REGION_KEY_VALUES_FILE_NAME = "regionKeyValues.txt";

    private int regionId;
    private String regionRootFolder;

    private StringIndexRegion() {
    };

    public static StringIndexRegion create(String regionRootFolder, int regionId) {
        StringIndexRegion ret = new StringIndexRegion();
        ret.regionId = regionId;
        ret.regionRootFolder = regionRootFolder;
        return ret;
    }

    public static int getRegionIdByIndexId(String indexId, int regionNumber) {
        return Math.abs(indexId.hashCode()) % regionNumber;
    }

    public static String getRegionFilesPrefix(String regionRootFolder, int regionId) {
        return regionRootFolder.concat(StringIndexRegion.REGION_PREFIX)
                .concat(String.valueOf(regionId)).concat("_");
    }

    public static String getRegionIndexFilePath(String regionRootFolder, int regionId, String indexIdName) {
        return StringIndexRegion.getRegionFilesPrefix(regionRootFolder, regionId)
                .concat(StringIndexRegion.REGION_INDEX_FILE_PREFIX).concat(indexIdName).concat(".txt");
    }

    public static String getRegionKeyMapFilePath(String regionRootFolder, int regionId) {
        return StringIndexRegion.getRegionFilesPrefix(regionRootFolder, regionId)
                .concat(StringIndexRegion.REGION_KEY_MAP_FILE_NAME);
    }

    public static String getRegionKeyValuesFilePath(String regionRootFolder, int regionId) {
        return StringIndexRegion.getRegionFilesPrefix(regionRootFolder, regionId)
                .concat(StringIndexRegion.REGION_KEY_VALUES_FILE_NAME);
    }

    public static boolean isRegionExist(String regionRootFolder, int regionId) {
        return FileUtil.isFileExist(StringIndexRegion
                .getRegionKeyMapFilePath(regionRootFolder, regionId));
    }
}