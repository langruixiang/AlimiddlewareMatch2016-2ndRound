package com.alibaba.middleware.race.constant;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangchao on 2016/7/12.
 */
public class FileConstant {

    public static final String UNSORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX = "unsorted_orderid_one_index_";
    
    public static final String SORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX = "sorted_orderid_one_index_";

    public static final String UNSORTED_BUYER_ID_ONE_INDEX_FILE_PREFIX = "unsorted_buyerid_one_index_";
    
    public static final String SORTED_BUYER_ID_ONE_INDEX_FILE_PREFIX = "sorted_buyerid_one_index_";

    public static String FILE_INDEX_BY_GOODID = "goodid_hash_";

    public static String FILE_RANK_BY_GOODID = "goodid_rank_";

    public static String FILE_ONE_INDEXING_BY_GOODID = "goodid_index_";

    public static String FILE_TWO_INDEXING_BY_GOODID = "goodid_two_index_";

    public static String FILE_RANK_BY_BUYERID = "buyerid_rank_";

    public static String FILE_TWO_INDEXING_BY_BUYERID = "buyerid_two_index_";

    public static String FILE_GOOD_HASH = "good_hash_";

    public static String FILE_BUYER_HASH = "buyer_hash_";

    public static Map<Integer, Integer> goodIdIndexRegionSizeMap = new ConcurrentHashMap<Integer, Integer>();

    public static Map<Integer, Integer> buyerIdIndexRegionSizeMap = new ConcurrentHashMap<Integer, Integer>();

    public static Map<Integer, Integer> orderIdIndexRegionSizeMap = new ConcurrentHashMap<Integer, Integer>();
}
