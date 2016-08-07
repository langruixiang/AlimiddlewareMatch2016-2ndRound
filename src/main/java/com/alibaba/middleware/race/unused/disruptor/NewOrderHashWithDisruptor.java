/**
 * OrderHashWithDisruptor.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.Config;
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
public class NewOrderHashWithDisruptor extends Thread {

    private Collection<String> orderFiles;
    private static final int writeThreadNum = 20;//TODO 每个handler的写线程数量
    private static final int orderIdHashFileNum = 3000;//TODO
    private static final int goodIdHashFileNum = 3000;//TODO
    private static final int buyerIdHashFileNum = 3000;//TODO
    private CountDownLatch countDownLatch;
    private int fileBeginNum;
    private static final int ringBufferSize = 1024 * 1024;// RingBuffer 大小，必须是 2 的 N 次方；
    
    public NewOrderHashWithDisruptor(Collection<String> orderFiles, CountDownLatch countDownLatch, int fileBeginNum) {
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
        StringEventHandler[] handlers = new StringEventHandler[3];
        int handlerCount = 0;

        CountDownLatch handlerWriteCountDownLatch = new CountDownLatch(3 * writeThreadNum);
        //orderidhash
        BufferedWriter[] orderIdHashBufferedWriters = new BufferedWriter[orderIdHashFileNum];
        try {
            for (int i = 0; i < orderIdHashFileNum; i++) {
                orderIdHashBufferedWriters[i] = new BufferedWriter(new FileWriter(Config.FIRST_DISK_PATH + FileConstant.UNSORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX + i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        handlers[handlerCount++] = new NewOrderIdHashHandler(0, orderIdHashFileNum, orderIdHashBufferedWriters, writeThreadNum, handlerWriteCountDownLatch);
        
        //buyeridhash
        BufferedWriter[] buyerIdHashBufferedWriters = new BufferedWriter[buyerIdHashFileNum];
        try {
            for (int i = 0; i < buyerIdHashFileNum; i++) {
                buyerIdHashBufferedWriters[i] = new BufferedWriter(new FileWriter(Config.SECOND_DISK_PATH + FileConstant.FILE_INDEX_BY_BUYERID + i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        handlers[handlerCount++] = new NewBuyerIdHashHandler(1, buyerIdHashFileNum, buyerIdHashBufferedWriters, writeThreadNum, handlerWriteCountDownLatch);

        //goodidhash
        BufferedWriter[] goodIdHashBufferedWriters = new BufferedWriter[goodIdHashFileNum];
        try {
            for (int i = 0; i < goodIdHashFileNum; i++) {
                goodIdHashBufferedWriters[i] = new BufferedWriter(new FileWriter(Config.THIRD_DISK_PATH + FileConstant.FILE_INDEX_BY_GOODID + i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        handlers[handlerCount++] = new NewGoodIdHashHandler(2, goodIdHashFileNum, goodIdHashBufferedWriters, writeThreadNum, handlerWriteCountDownLatch);

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
            StringEventPublisher producer = new StringEventPublisher(disruptor.getRingBuffer());
            for (int i = 0; i < writeThreadNum; ++i) {
                producer.publish("", 0, 0);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        disruptor.shutdown();
        try {
            handlerWriteCountDownLatch.await();
            closeBufferedWirters(orderIdHashBufferedWriters);
            closeBufferedWirters(buyerIdHashBufferedWriters);
            closeBufferedWirters(goodIdHashBufferedWriters);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
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
