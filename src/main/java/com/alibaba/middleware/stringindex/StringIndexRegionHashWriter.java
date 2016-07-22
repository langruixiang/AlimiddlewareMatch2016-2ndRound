/**
 * StringIndexRegionHashWriter.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * @author wangweiwei
 *
 */
public class StringIndexRegionHashWriter extends Thread{

    private int regionId;

    private LinkedBlockingQueue<String> lineQueue;
    
    private BufferedWriter regionKeyValuesFileBW;

    private Map<String, Integer> keyMap;

    private String regionKeyMapFilePath;
    private String regionKeyValuesFileName;
    
    CountDownLatch hashWriterCountDownLatch;

    public StringIndexRegionHashWriter(String regionRootFolder, int initKeyMapCapacity,
            CountDownLatch hashWriterCountDownLatch, int regionId) {
        this.hashWriterCountDownLatch = hashWriterCountDownLatch;
        this.regionId = regionId;
        this.lineQueue = new LinkedBlockingQueue<String>();
        this.regionKeyValuesFileName = StringIndexRegion.getRegionKeyValuesFilePath(regionRootFolder, regionId);
        this.keyMap = new HashMap<String, Integer>(initKeyMapCapacity);
        this.regionKeyMapFilePath = StringIndexRegion.getRegionKeyMapFilePath(regionRootFolder, regionId);
    }

    
    @Override
    public void run() {
        try {
            this.regionKeyValuesFileBW = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(regionKeyValuesFileName), StringIndex.ENCODING));
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                String line = lineQueue.take();
                if (!line.isEmpty()) {
                    String[] keyValues = line.split("\t");
                    for (int i = 0; i < keyValues.length; i++) {
                        String[] keyValue = keyValues[i].split(":");
                        if (!keyMap.containsKey(keyValue[0])) {
                            keyMap.put(keyValue[0], keyMap.size());
                        }
                    }
                    regionKeyValuesFileBW.write(line.concat("\n"));
                } else {
                    regionKeyValuesFileBW.close();
                    writeKeyMapFile();
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        hashWriterCountDownLatch.countDown();
    }

    public void sendLine(String line) {
        lineQueue.offer(line);
    }
    
    public void sendIndexOverSignal() {
        lineQueue.offer("");
    }

    private void writeKeyMapFile() {
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
}
