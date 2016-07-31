package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.model.Good;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangchao on 2016/8/1.
 */
public class GoodCache {
    public static Map<String, Good> goodMap = new ConcurrentHashMap<String, Good>();
}
