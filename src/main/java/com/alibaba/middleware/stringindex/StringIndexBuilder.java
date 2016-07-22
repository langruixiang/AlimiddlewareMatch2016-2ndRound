/**
 * StringIndexBuilder.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author wangweiwei
 *
 */
public class StringIndexBuilder extends Thread {

    private String regionRootFolder;
    private int regionNumber;
    private String indexIdName;
    private int initKeyMapCapacity;
    private CountDownLatch countDownLatch;
    
    private int regionBuilderThreadPoolSize;
    private ExecutorService regionBuilderThreadPool;
    private Map<Integer, StringIndexRegionBuilder> regionBuilders;
    private CountDownLatch regionBuilderCountDownLatch;

    public StringIndexBuilder(String regionRootFolder, int regionNumber, String indexIdName, int initKeyMapCapacity, CountDownLatch countDownLatch, int regionBuilderThreadPoolSize) {
        if (!regionRootFolder.endsWith("/")) {
            regionRootFolder = regionRootFolder.concat("/");
        }
        this.regionRootFolder = regionRootFolder;
        this.regionNumber = regionNumber;
        this.initKeyMapCapacity = initKeyMapCapacity;
        this.indexIdName = indexIdName;
        this.countDownLatch = countDownLatch;
        this.regionBuilderThreadPoolSize = regionBuilderThreadPoolSize;
    }

    @Override
    public void run(){
        prepare();
        try {
            regionBuilderCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        regionBuilderThreadPool.shutdown();
        countDownLatch.countDown();//完成工作，计数器减一
        System.out.println("StringIndexBuilder build end~");
    }
    
    private void prepare() {
        this.regionBuilders = new HashMap<Integer, StringIndexRegionBuilder>(regionNumber, 1);
        this.regionBuilderCountDownLatch = new CountDownLatch(regionNumber);
        regionBuilderThreadPool = Executors.newFixedThreadPool(regionBuilderThreadPoolSize);
        for (int i = 0; i < regionNumber; i++) {
            StringIndexRegionBuilder regionBuilder = new StringIndexRegionBuilder(regionRootFolder, i, indexIdName, initKeyMapCapacity, regionBuilderCountDownLatch);
            regionBuilders.put(i, regionBuilder);
            regionBuilderThreadPool.execute(regionBuilder);
        }
    }
}
