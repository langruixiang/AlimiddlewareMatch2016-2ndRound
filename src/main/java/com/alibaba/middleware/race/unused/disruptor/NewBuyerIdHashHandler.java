/**
 * BuyerIdHashHandler.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author wangweiwei
 *
 */
public class NewBuyerIdHashHandler extends StringEventHandler implements Runnable{
    private BufferedWriter[] bufferedWriters;
    
    private int hashFileNum;
    
    private BlockingQueue<StringEvent> events;

    private CountDownLatch latch;

    public NewBuyerIdHashHandler(int handlerId, int hashFileNum, BufferedWriter[] bufferedWriters, int writeThreadNum, CountDownLatch latch) {
        super(handlerId);
        this.hashFileNum = hashFileNum;
        this.bufferedWriters = bufferedWriters;
        this.latch = latch;
        this.events = new LinkedBlockingQueue<StringEvent>();
        for (int i = 0; i < writeThreadNum; ++i) {
            new Thread(this).start();
        }
    }

    @Override
    public void onEvent(StringEvent event, long sequence, boolean endOfBatch)
            throws Exception {
        events.offer(event);
//        System.out.println("[" + handlerId + "]"  + " : " + event.content);
    }

    @Override
    public void run() {
        while (true) {
            try {
                StringEvent event = events.take();
                String line = event.content;
                if (line.isEmpty()) {
                    break;
                }
                String buyeridStr = null;
                String createtime = null;
                StringTokenizer stringTokenizer = new StringTokenizer(line, "\t");
                while (stringTokenizer.hasMoreElements()) {
                    StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                    String key = keyValue.nextToken();
                    String value = keyValue.nextToken();
                    if ("buyerid".equals(key)) {
                        buyeridStr = value;
                    } else if ("createtime".equals(key)) {
                        createtime = value;
                    }
                    if (buyeridStr != null && createtime != null) {
                        int hashFileIndex = (int) (Math.abs(buyeridStr.hashCode()) % hashFileNum);
//                        if (handlerId == hashFileIndex % hashHandlerNum) {
                            String content = buyeridStr + ":" + createtime + ":" + event.fileNo + ":" + event.position + '\n';
                            synchronized (bufferedWriters[hashFileIndex]) {
                                bufferedWriters[hashFileIndex].write(content);
                            }
//                        }
                        break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        latch.countDown();
    }
}
