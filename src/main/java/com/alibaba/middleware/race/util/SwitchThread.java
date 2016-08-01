package com.alibaba.middleware.race.util;

import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/27.
 */
public class SwitchThread extends Thread{

    private CountDownLatch countDownLatch;

    public SwitchThread(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        try {
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 50 * 60 * 1000) {
                System.gc();
                Thread.sleep(5 * 60 * 1000);
            }
            System.gc();
            while (System.currentTimeMillis() - startTime < 59 * 60 * 1000) {
                Thread.sleep(20 * 1000);
            }
//            Thread.sleep(3590000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        countDownLatch.countDown();
    }
}
