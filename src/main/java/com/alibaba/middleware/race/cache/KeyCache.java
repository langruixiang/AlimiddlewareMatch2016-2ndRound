package com.alibaba.middleware.race.cache;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangchao on 2016/7/21.
 */
public class KeyCache {
    
    public static final int INIT_KEY_CACHE_CAPACITY = 20;

    public static Object EMPTY_OBJECT = new Object();

    public static ConcurrentHashMap<String, Object> orderKeyCache = new ConcurrentHashMap<String, Object>(INIT_KEY_CACHE_CAPACITY);

    public static ConcurrentHashMap<String, Object> buyerKeyCache = new ConcurrentHashMap<String, Object>(INIT_KEY_CACHE_CAPACITY);

    public static ConcurrentHashMap<String, Object> goodKeyCache = new ConcurrentHashMap<String, Object>(INIT_KEY_CACHE_CAPACITY);

}
