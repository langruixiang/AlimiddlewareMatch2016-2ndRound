package com.alibaba.middleware.race.posinfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.alibaba.middleware.race.model.PosInfo;

/**
 * Created by jiangchao on 2016/7/24.
 */
public class NewOneIndexCache {

    public static Map<String, PosInfo> buyerOneIndexCache = new ConcurrentHashMap<String, PosInfo>();

    public static Map<String, PosInfo> goodOneIndexCache = new ConcurrentHashMap<String, PosInfo>();
}
