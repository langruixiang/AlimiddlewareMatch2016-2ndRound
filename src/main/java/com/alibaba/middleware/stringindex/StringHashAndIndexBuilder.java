/**
 * StringHashAndIndexBuilder.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.util.FileUtil;

/**
 * @author wangweiwei
 *
 */
public class StringHashAndIndexBuilder extends Thread {

    private Map<String, StringHashAndIndexWriter> hashAndIndexWriters;
    
    private Collection<String> originalFiles;
    private String regionRootFolder;
    private CountDownLatch countDownLatch;
    private String hashAndIndexIdName;
    private int regionNumber;
    
    public StringHashAndIndexBuilder(Collection<String> originalFiles, String storeFolder,
            int regionNumber, String hashAndIndexIdName, CountDownLatch countDownLatch) {
        this.originalFiles = originalFiles;
        if (!storeFolder.endsWith("/")) {
            storeFolder = storeFolder.concat("/");
        }
        this.regionRootFolder = storeFolder;
        this.countDownLatch = countDownLatch;
        this.hashAndIndexIdName = hashAndIndexIdName;
        this.regionNumber = regionNumber;
        this.hashAndIndexWriters = new HashMap<String, StringHashAndIndexWriter>(regionNumber);
    }

    public void build() {
        try {
            createAllNecessaryDirs();
            for (String file : originalFiles) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    buildWithLine(line);
                }
                br.close();
            }
            indexOver();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createAllNecessaryDirs() {
        FileUtil.createDir(regionRootFolder);
//        for (int i = 0; i < StringIndexRegion.REGION_NUM; ++i) {
//            StringIndexRegion.getRegionDirPath(String.valueOf(i));
//        }
    }

    private void indexOver() {
        for (Entry<String, StringHashAndIndexWriter> entry : hashAndIndexWriters.entrySet()) {
            entry.getValue().sendIndexOverSignal();
        }
    }

    private void buildWithLine(String line) throws IOException {
        StringIndex stringIndex = new StringIndex();
        String[] keyValues = line.split("\t");
        for (int i = 0; i < keyValues.length; i++) {
            String[] keyValue = keyValues[i].split(":");
            if (hashAndIndexIdName.equals(keyValue[0])) {
                stringIndex.setIndexIdName(keyValue[0]);
                stringIndex.setIndexId(keyValue[1]);
            }
            stringIndex.addKeyValue(keyValue[0], keyValue[1]);
        }
        String regionId = StringIndexRegion.getRegionIdByIndexId(stringIndex.getIndexId(), regionNumber);
        StringHashAndIndexWriter regionWriter = hashAndIndexWriters.get(regionId);
        if (regionWriter == null) {
            regionWriter = new StringHashAndIndexWriter(regionRootFolder, regionId, hashAndIndexIdName);
            hashAndIndexWriters.put(regionId, regionWriter);
            regionWriter.start();
        }
        regionWriter.sendIndex(stringIndex);
    }

    @Override
    public void run(){
        build();
        System.out.println("StringHashAndIndexBuilder build end~");
        countDownLatch.countDown();
    }
}
