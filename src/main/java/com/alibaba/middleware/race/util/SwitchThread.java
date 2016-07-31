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
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        countDownLatch.countDown();
    }
}
