/**
 * GoodIdHashHandler.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.disruptor;

import java.io.BufferedWriter;
import java.util.StringTokenizer;

/**
 * @author wangweiwei
 *
 */
public class GoodIdHashHandler extends StringEventHandler {
    private BufferedWriter[] bufferedWriters;
    
    private int hashFileNum;

    private int  hashHandlerNum;
    
    public GoodIdHashHandler(int handlerId, int hashFileNum, BufferedWriter[] bufferedWriters, int hashHandlerNum) {
        super(handlerId);
        this.hashFileNum = hashFileNum;
        this.bufferedWriters = bufferedWriters;
        this.hashHandlerNum = hashHandlerNum;
    }
    
//    @Override
//    public void onEvent(StringEvent event, long sequence, boolean endOfBatch)
//            throws Exception {
//        int hashFileIndex = (int) (Math.abs(event.goodId.hashCode()) % hashFileNum);
//        if (handlerId == hashFileIndex % hashHandlerNum) {
//            String content = event.goodId + ":" + event.orderId + ":" + event.fileNo + ":" + event.position + '\n';
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
        
        String orderIdStr = null;
        String goodIdStr = null;
        StringTokenizer stringTokenizer = new StringTokenizer(line, "\t");
        while (stringTokenizer.hasMoreElements()) {
            StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
            String key = keyValue.nextToken();
            String value = keyValue.nextToken();

            if ("orderid".equals(key)) {
                orderIdStr = value;
            } else if ("goodid".equals(key)) {
                goodIdStr = value;
            }
            if (orderIdStr != null && goodIdStr != null) {
                int hashFileIndex = (int) (Math.abs(goodIdStr.hashCode()) % hashFileNum);
                if (handlerId == hashFileIndex % hashHandlerNum) {
                    String content = goodIdStr + ":" + orderIdStr + ":" + event.fileNo + ":" + event.position + '\n';
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
