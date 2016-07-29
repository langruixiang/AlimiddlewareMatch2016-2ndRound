/**
 * OrderIdIndexBuilder.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused;

import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.stringindex.StringIndexBuilder;

/**
 * @author wangweiwei
 *
 */
public class NewOrderIdIndexBuilder extends StringIndexBuilder {
    CountDownLatch awaitLatch;

    public NewOrderIdIndexBuilder(CountDownLatch awaitLatch, CountDownLatch countDownLatch) {
        super(NewOrderIdIndexConfig.REGION_ROOT_FOLDER,
                NewOrderIdIndexConfig.REGION_NUMBER,
                NewOrderIdIndexConfig.HASH_INDEX_INDEX_ID_NAME,
                NewOrderIdIndexConfig.INIT_KEY_MAP_CAPACITY,
                countDownLatch,
                NewOrderIdIndexConfig.REGION_INDEX_BUILDER_THREAD_POOL_SIZE);
        this.awaitLatch = awaitLatch;
    }
    @Override
    public void run() {
        try {
            awaitLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        super.run();
    }
}
