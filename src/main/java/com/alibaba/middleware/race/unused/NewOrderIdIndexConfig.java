/**
 * OrderIdQueryConfig.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused;

/**
 * @author wangweiwei
 *
 */
public class NewOrderIdIndexConfig {
    public static String REGION_ROOT_FOLDER = "OrderIdIndexRegion/";

    public static final int REGION_NUMBER = 200;
    public static final String HASH_INDEX_INDEX_ID_NAME = "orderid";
    public static final int HASH_WRITER_THREAD_POOL_SIZE = 10;
    public static final int INIT_KEY_MAP_CAPACITY = 20;
    public static final int REGION_INDEX_BUILDER_THREAD_POOL_SIZE = 10;

    public static final int MAX_INDEX_CACHE_SIZE = 1000;
    public static final int CACHE_NUM_PER_MISS = 100;

}
