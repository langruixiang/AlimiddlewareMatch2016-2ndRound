package com.alibaba.middleware.race.constant;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangchao on 2016/7/12.
 */
public class FileConstant {

    public static String FILE_INDEX_BY_ORDERID = "orderid_hash_";

    public static String FILE_INDEX_BY_BUYERID = "buyerid_hash_";

    public static String FILE_INDEX_BY_GOODID = "goodid_hash_";

    public static String FILE_ONE_INDEXING_BY_ORDERID = "orderid_index_";

    public static String FILE_TWO_INDEXING_BY_ORDERID = "orderid_two_index_";

    public static String FILE_RANK_BY_GOODID = "goodid_rank_";

    public static String FILE_ONE_INDEXING_BY_GOODID = "goodid_index_";

    public static String FILE_TWO_INDEXING_BY_GOODID = "goodid_two_index_";

    public static String FILE_RANK_BY_BUYERID = "buyerid_rank_";

    public static String FILE_ONE_INDEXING_BY_BUYERID = "buyerid_index_";

    public static String FILE_TWO_INDEXING_BY_BUYERID = "buyerid_two_index_";

    public static String FILE_GOOD_HASH = "good_hash_";

    public static String FILE_BUYER_HASH = "buyer_hash_";

    //public static int FILE_NUMS = 200;

    public static int FILE_ORDER_NUMS = 5000;

    public static int FILE_GOOD_NUMS = 2000;

    public static int FILE_BUYER_NUMS = 2000;

    public static int MAX_CONCURRENT = 200;

    public static String FIRST_DISK_PATH = "";

    public static String SECOND_DISK_PATH = "";

    public static String THIRD_DISK_PATH = "";

    public static Map<Integer, Integer> goodIdIndexRegionSizeMap = new ConcurrentHashMap<Integer, Integer>();

    public static Map<Integer, Integer> buyerIdIndexRegionSizeMap = new ConcurrentHashMap<Integer, Integer>();

    public static Map<Integer, Integer> orderIdIndexRegionSizeMap = new ConcurrentHashMap<Integer, Integer>();
}
