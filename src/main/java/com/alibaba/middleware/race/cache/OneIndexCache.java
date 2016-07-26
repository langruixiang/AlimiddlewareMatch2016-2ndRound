package com.alibaba.middleware.race.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.alibaba.middleware.race.file.PosInfo;

/**
 * Created by jiangchao on 2016/7/24.
 */
public class OneIndexCache {

    public static Map<String, PosInfo> buyerOneIndexCache = new ConcurrentHashMap<String, PosInfo>();

    public static Map<String, PosInfo> goodOneIndexCache = new ConcurrentHashMap<String, PosInfo>();
}
