/**
 * OrderIdHashHandler.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import java.io.BufferedWriter;
import java.util.StringTokenizer;

/**
 * @author wangweiwei
 *
 */
public class OrderIdHashHandler extends StringEventHandler {
    private BufferedWriter[] bufferedWriters;
    
    private int hashFileNum;
    
    private int hashHandlerNum;

    public OrderIdHashHandler(int handlerId, int hashFileNum, BufferedWriter[] bufferedWriters, int hashHandlerNum) {
        super(handlerId);
        this.hashFileNum = hashFileNum;
        this.bufferedWriters = bufferedWriters;
        this.hashHandlerNum = hashHandlerNum;
    }
    
//    @Override
//    public void onEvent(StringEvent event, long sequence, boolean endOfBatch)
//            throws Exception {
//        int hashFileIndex = (int) (event.orderId % hashFileNum);
//        if (handlerId == hashFileIndex % hashHandlerNum) {
//            String content = event.orderId + ":" + event.fileNo + ":" + event.position + '\n';
//            synchronized (bufferedWriters[hashFileIndex]) {
//                bufferedWriters[hashFileIndex].write(content);
//            }
//        }
////        System.out.println("[" + handlerId + "]"  + " : " + event.content);
//    }
    
    @Override
    public void onEvent(StringEvent event, long sequence, boolean endOfBatch)
            throws Exception {
        String line = event.content;
        StringTokenizer stringTokenizer = new StringTokenizer(line, "\t");
        while (stringTokenizer.hasMoreElements()) {
            StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
            String key = keyValue.nextToken();
            String value = keyValue.nextToken();
            if ("orderid".equals(key)) {
                long orderId = Long.valueOf(value);
                int hashFileIndex = (int) (orderId % hashFileNum);
                if (handlerId == hashFileIndex % hashHandlerNum) {
                    String content = orderId + ":" + event.fileNo + ":" + event.position + '\n';
                    synchronized (bufferedWriters[hashFileIndex]) {
                        bufferedWriters[hashFileIndex].write(content);
                    }
                }
                break;
            }
        }

//        System.out.println("[" + handlerId + "]"  + " : " + event.content);
    }
}
