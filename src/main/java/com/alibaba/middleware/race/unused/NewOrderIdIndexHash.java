/**
 * StringIndexRegionHash.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.stringindex.StringIndexHash;

/**
 * @author wangweiwei
 *
 */
public class NewOrderIdIndexHash extends StringIndexHash {
    public NewOrderIdIndexHash(Collection<String> srcFiles, CountDownLatch countDownLatch) {
        super(srcFiles, NewOrderIdIndexConfig.REGION_ROOT_FOLDER,
                NewOrderIdIndexConfig.REGION_NUMBER,
                NewOrderIdIndexConfig.HASH_INDEX_INDEX_ID_NAME,
                NewOrderIdIndexConfig.INIT_KEY_MAP_CAPACITY,
                countDownLatch,
                NewOrderIdIndexConfig.HASH_WRITER_THREAD_POOL_SIZE);
    }
}
