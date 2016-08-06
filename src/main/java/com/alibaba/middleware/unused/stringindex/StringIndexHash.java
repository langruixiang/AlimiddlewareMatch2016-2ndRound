/**
 * StringIndexRegionHash.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.unused.stringindex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private int initKeyMapCapacity;
    private CountDownLatch countDownLatch;
    
    private int hashWriterThreadPoolSize;
    private ExecutorService hashWriterThreadPool;
    private Map<Integer, StringIndexRegionHashWriter> hashWriters;
    private CountDownLatch hashWriterCountDownLatch;

    public StringIndexHash(Collection<String> srcFiles,  String regionRootFolder, int regionNumber, String hashIdName,
            int initKeyMapCapacity, CountDownLatch countDownLatch, int hashWriterThreadPoolSize) {
        this.srcFiles = srcFiles;
        if (!regionRootFolder.endsWith("/")) {
            regionRootFolder = regionRootFolder.concat("/");
        }
        this.regionRootFolder = regionRootFolder;
        this.regionNumber = regionNumber;
        this.hashIdName = hashIdName;
        this.initKeyMapCapacity = initKeyMapCapacity;
        this.countDownLatch = countDownLatch;
        this.hashWriterThreadPoolSize = hashWriterThreadPoolSize;
    }

    @Override
    public void run() {
        prepare();
        startHash();
        try {
            hashWriterCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        hashWriterThreadPool.shutdown();
        countDownLatch.countDown();//完成工作，计数器减一
        System.out.println("StringIndexHash end~");
    }
    
    /**
     * 
     */
    private void prepare() {
        createAllNecessaryDirs();
        createAllHashWriters();
    }

    private void createAllNecessaryDirs() {
        FileUtil.createDir(regionRootFolder);
    //    for (int i = 0; i < StringIndexRegion.REGION_NUM; ++i) {
    //        StringIndexRegion.getRegionDirPath(String.valueOf(i));
    //    }
    }

    private void createAllHashWriters () {
        this.hashWriters = new HashMap<Integer, StringIndexRegionHashWriter>(regionNumber, 1);
        this.hashWriterCountDownLatch = new CountDownLatch(regionNumber);
        hashWriterThreadPool = Executors.newFixedThreadPool(hashWriterThreadPoolSize);
        for (int i = 0; i < regionNumber; i++) {
            StringIndexRegionHashWriter hashWriter = new StringIndexRegionHashWriter(regionRootFolder, initKeyMapCapacity, hashWriterCountDownLatch, i);
            hashWriters.put(i, hashWriter);
            hashWriterThreadPool.execute(hashWriter);
        }
    }

    private void startHash() {
        try {
            createAllNecessaryDirs();
            for (String file : srcFiles) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    hashLine(line);
                }
                br.close();
            }
            indexOver();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    private void indexOver() {
        for (Entry<Integer, StringIndexRegionHashWriter> entry : hashWriters.entrySet()) {
            entry.getValue().sendIndexOverSignal();
        }
    }

    private void hashLine(String line) {
        int hashIdStartIndex = line.indexOf(hashIdName.concat(":")) + hashIdName.length() + 1;
        if (hashIdStartIndex < 0) {
            return;
        }
        int hashIdEndIndex = line.indexOf('\t', hashIdStartIndex);
        if (hashIdEndIndex < 0) {
            return;
        }
        String hashId = line.substring(hashIdStartIndex, hashIdEndIndex);
        int regionId = StringIndexHash.getRegionIdByHashId(hashId, regionNumber);

        StringIndexRegionHashWriter hashWriter = hashWriters.get(regionId);
        hashWriter.sendLine(line);
    }
    
    private void writeLineDirectly(String line, int regionId) {
        
    }

    public static int getRegionIdByHashId(String hashId, int regionNumber) {
        return Math.abs(hashId.hashCode()) % regionNumber;
    }
}
