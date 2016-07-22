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
    
    private String regionKeyValuesFileName;
    
    CountDownLatch hashWriterCountDownLatch;

    public StringIndexRegionHashWriter(String regionRootFolder,
            CountDownLatch hashWriterCountDownLatch, int regionId) {
        this.hashWriterCountDownLatch = hashWriterCountDownLatch;
        this.regionId = regionId;
        this.lineQueue = new LinkedBlockingQueue<String>();
        this.regionKeyValuesFileName = StringIndexRegion.getRegionKeyValuesFilePath(regionRootFolder, regionId);
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
                    regionKeyValuesFileBW.write(line.concat("\n"));
                } else {
                    regionKeyValuesFileBW.close();
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
}
