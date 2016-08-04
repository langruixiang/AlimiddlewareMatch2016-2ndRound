/**
 * OrderHashWithDisruptor.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.disruptor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.cache.FileNameCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * @author wangweiwei
 *
 */
public class OrderHashWithDisruptor extends Thread {

    private Collection<String> orderFiles;
    private static final int orderIdHashFileNum = 300;//TODO
    private static final int goodIdHashFileNum = 300;//TODO
    private static final int buyerIdHashFileNum = 300;//TODO
    private CountDownLatch countDownLatch;
    private int fileBeginNum;
    public static final int orderIdHashHandlerNum = 10;
    public static final int goodIdHashHandlerNum = 10;
    public static final int buyerIdHashHandlerNum = 10;
    private static final int ringBufferSize = 1024 * 1024;// RingBuffer 大小，必须是 2 的 N 次方；
    
    public OrderHashWithDisruptor(Collection<String> orderFiles, CountDownLatch countDownLatch, int fileBeginNum) {
        this.orderFiles = orderFiles;
        this.countDownLatch = countDownLatch;
        this.fileBeginNum = fileBeginNum;
    }
    
    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        EventFactory<StringEvent> eventFactory = new StringEventFactory();
        SimpleThreadFactory threadFactory = SimpleThreadFactory.INSTANCE;
        Disruptor<StringEvent> disruptor = new Disruptor<StringEvent>(eventFactory,
                        ringBufferSize, threadFactory, ProducerType.MULTI, new YieldingWaitStrategy());
        
        // --------------------------initialize handlers
        StringEventHandler[] handlers = new StringEventHandler[orderIdHashHandlerNum + goodIdHashHandlerNum + buyerIdHashHandlerNum];
        int handlerCount = 0;

        //orderidhash
        BufferedWriter[] orderIdHashBufferedWriters = new BufferedWriter[orderIdHashFileNum];
        try {
            for (int i = 0; i < orderIdHashFileNum; i++) {
                orderIdHashBufferedWriters[i] = new BufferedWriter(new FileWriter(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_INDEX_BY_ORDERID + i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < orderIdHashHandlerNum; ++i) {
            handlers[handlerCount++] = new OrderIdHashHandler(i, orderIdHashFileNum, orderIdHashBufferedWriters, orderIdHashHandlerNum);
        }
        
        //buyeridhash
        BufferedWriter[] buyerIdHashBufferedWriters = new BufferedWriter[buyerIdHashFileNum];
        try {
            for (int i = 0; i < buyerIdHashFileNum; i++) {
                buyerIdHashBufferedWriters[i] = new BufferedWriter(new FileWriter(FileConstant.SECOND_DISK_PATH + FileConstant.FILE_INDEX_BY_BUYERID + i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < buyerIdHashHandlerNum; ++i) {
            handlers[handlerCount++] = new BuyerIdHashHandler(i, buyerIdHashFileNum, buyerIdHashBufferedWriters, buyerIdHashHandlerNum);
        }

        //goodidhash
        BufferedWriter[] goodIdHashBufferedWriters = new BufferedWriter[goodIdHashFileNum];
        try {
            for (int i = 0; i < goodIdHashFileNum; i++) {
                goodIdHashBufferedWriters[i] = new BufferedWriter(new FileWriter(FileConstant.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < goodIdHashHandlerNum; ++i) {
            handlers[handlerCount++] = new GoodIdHashHandler(i, goodIdHashFileNum, goodIdHashBufferedWriters, goodIdHashHandlerNum);
        }

        disruptor.handleEventsWith(handlers);
        disruptor.start();
        
        // --------------------------initialize publishers
        int publisherNum = orderFiles.size();
        final CountDownLatch producerLatch = new CountDownLatch(publisherNum);
        int tmpFileNo = fileBeginNum;
        for (String orderFile : orderFiles) {
            FileNameCache.fileNameMap.put(tmpFileNo, orderFile);
            OrderLinePublisher publisher = new OrderLinePublisher(disruptor.getRingBuffer(), producerLatch, orderFile, tmpFileNo);
            publisher.start();
            ++tmpFileNo;
        }

        try {
            producerLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        disruptor.shutdown();
        try {
            closeBufferedWirters(orderIdHashBufferedWriters);
            closeBufferedWirters(buyerIdHashBufferedWriters);
            closeBufferedWirters(goodIdHashBufferedWriters);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.countDownLatch.countDown();
        System.out.println("OrderHashWithDisruptor total used time : " + (System.currentTimeMillis() - startTime));
    }

    /**
     * @param goodIdHashBufferedWriters
     * @throws IOException 
     */
    private void closeBufferedWirters(BufferedWriter[] bufferedWriters) throws IOException {
        if (bufferedWriters == null) return;
        for (BufferedWriter writer : bufferedWriters) {
            writer.close();
        }
    }

}
