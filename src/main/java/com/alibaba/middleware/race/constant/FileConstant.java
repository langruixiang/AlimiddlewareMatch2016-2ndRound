package com.alibaba.middleware.race.constant;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jiangchao on 2016/7/12.
 */
public class FileConstant {

    public static String FILE_INDEX_BY_ORDERID = "orderid_hash_";

    public static String FILE_INDEX_BY_BUYERID = "buyerid_hash_";

    public static String FILE_INDEX_BY_GOODID = "goodid_hash_";


    public static String FILE_ONE_INDEXING_BY_GOODID = "goodid_index_";

    public static String FILE_TWO_INDEXING_BY_GOODID = "goodid_two_index_";

    public static String FILE_ONE_INDEXING_BY_BUYERID = "buyerid_index_";

    public static String FILE_TWO_INDEXING_BY_BUYERID = "buyerid_two_index_";

    public static String FILE_GOOD_HASH = "good_hash_";

    public static String FILE_BUYER_HASH = "buyer_hash_";

    public static int FILE_NUMS = 25;

    public static Map<Integer, Integer> goodIdIndexRegionSizeMap = new HashMap<Integer, Integer>();

    public static Map<Integer, Integer> buyerIdIndexRegionSizeMap = new HashMap<Integer, Integer>();
}
