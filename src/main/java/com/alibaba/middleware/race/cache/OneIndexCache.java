package com.alibaba.middleware.race.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by jiangchao on 2016/7/24.
 */
public class OneIndexCache {

    public static Map<String, Long> buyerOneIndexCache = new ConcurrentHashMap<String, Long>();

    public static Map<String, Long> goodOneIndexCache = new ConcurrentHashMap<String, Long>();

    public static Map<String, List<Long>> goodidOneIndexCache = new ConcurrentHashMap<String, List<Long>>();
}
