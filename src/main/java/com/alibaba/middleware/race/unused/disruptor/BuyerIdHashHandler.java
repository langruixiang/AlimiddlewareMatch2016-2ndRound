/**
 * BuyerIdHashHandler.java
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
public class BuyerIdHashHandler extends StringEventHandler {
    private BufferedWriter[] bufferedWriters;
    
    private int hashFileNum;
    
    private int hashHandlerNum;

    public BuyerIdHashHandler(int handlerId, int hashFileNum, BufferedWriter[] bufferedWriters, int hashHandlerNum) {
        super(handlerId);
        this.hashFileNum = hashFileNum;
        this.bufferedWriters = bufferedWriters;
        this.hashHandlerNum = hashHandlerNum;
    }

//    @Override
//    public void onEvent(StringEvent event, long sequence, boolean endOfBatch)
//            throws Exception {
//        int hashFileIndex = (int) (Math.abs(event.buyerId.hashCode()) % hashFileNum);
//        if (handlerId == hashFileIndex % hashHandlerNum) {
//            String content = event.buyerId + ":" + event.createtime + ":" + event.fileNo + ":" + event.position + '\n';
//            synchronized (bufferedWriters[hashFileIndex]) {
//                bufferedWriters[hashFileIndex].write(content);
//            }
//        }
//
////        System.out.println("[" + handlerId + "]"  + " : " + event.content);
//    }

    @Override
    public void onEvent(StringEvent event, long sequence, boolean endOfBatch)
            throws Exception {
        String line = event.content;
        
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
                if (handlerId == hashFileIndex % hashHandlerNum) {
                    String content = buyeridStr + ":" + createtime + ":" + event.fileNo + ":" + event.position + '\n';
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
