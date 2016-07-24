/**
 * StringIndexRegionHash.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
public class StringIndexHash extends Thread {
    private Collection<String> srcFiles;
    private String regionRootFolder;
    private int regionNumber;
    private String hashIdName;
    private CountDownLatch countDownLatch;
    
    private Map<String, Integer> keyMap;
    private BufferedWriter[] regionWriters;

    public StringIndexHash(Collection<String> srcFiles,  String regionRootFolder, int regionNumber, String hashIdName,
            int initKeyMapCapacity, CountDownLatch countDownLatch, int hashWriterThreadPoolSize) {
        this.srcFiles = srcFiles;
        if (!regionRootFolder.endsWith("/")) {
            regionRootFolder = regionRootFolder.concat("/");
        }
        this.regionRootFolder = regionRootFolder;
        this.regionNumber = regionNumber;
        this.hashIdName = hashIdName;
        this.countDownLatch = countDownLatch;
        this.keyMap = new HashMap<String, Integer>(initKeyMapCapacity);
        this.regionWriters = new BufferedWriter[regionNumber];
    }

    @Override
    public void run() {
        prepare();
        try {
            startHash();
            for (BufferedWriter writer : regionWriters) {
                if (writer != null) {
                    writer.close();
                }
            }
            writeKeyMapFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        countDownLatch.countDown();//完成工作，计数器减一
        System.out.println("StringIndexHash end~");
    }
    
    /**
     * 
     */
    private void prepare() {
        createAllNecessaryDirs();
    }

    private void createAllNecessaryDirs() {
        FileUtil.createDir(regionRootFolder);
    //    for (int i = 0; i < StringIndexRegion.REGION_NUM; ++i) {
    //        StringIndexRegion.getRegionDirPath(String.valueOf(i));
    //    }
    }

    private void startHash() throws IOException {
        createAllNecessaryDirs();
        for (String file : srcFiles) {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line = null;
            while ((line = br.readLine()) != null) {
                hashLine(line);
            }
            br.close();
        }
    }

    private void writeKeyMapFile() {
        String regionKeyMapFilePath = StringIndexRegion.getRegionKeyMapFilePath(regionRootFolder, 0);
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(regionKeyMapFilePath),
                    StringIndex.ENCODING));
            for (Entry<String, Integer> entry : keyMap.entrySet()) {
                writer.write(entry.getKey().concat(":")
                        .concat(entry.getValue().toString()).concat("\n"));
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void hashLine(String line) throws IOException {
        BufferedWriter regionWriter = null;
        String[] keyValues = line.split("\t");
        for (int i = 0; i < keyValues.length; ++i) {
            String[] keyValue = keyValues[i].split(":");
            if (keyValue.length < 2) {
                System.out.println(line);
            }
            if (!keyMap.containsKey(keyValue[0])) {
                keyMap.put(keyValue[0], keyMap.size());
            }
            if (hashIdName.equals(keyValue[0])) {
                int regionId = StringIndexHash.getRegionIdByHashId(keyValue[1], regionNumber);
                regionWriter = regionWriters[regionId];
                if (regionWriter == null) {
                    String regionKeyValuesFileName = StringIndexRegion.getRegionKeyValuesFilePath(regionRootFolder, regionId);
                    regionWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(regionKeyValuesFileName), StringIndex.ENCODING));
                    regionWriters[regionId] = regionWriter;
                }
                
            }
        }
        if (regionWriter != null) {
            regionWriter.write(line.concat("\n"));
        }
        
    }

    public static int getRegionIdByHashId(String hashId, int regionNumber) {
        return Math.abs(hashId.hashCode()) % regionNumber;
    }
}
