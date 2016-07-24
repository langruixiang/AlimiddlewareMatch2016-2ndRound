package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;
import com.alibaba.middleware.race.util.FileUtil;
import com.alibaba.middleware.stringindex.StringIndex;
import com.alibaba.middleware.stringindex.StringIndexCache;
import com.alibaba.middleware.stringindex.StringIndexRegion;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiangchao on 2016/7/17.
 */
public class OrderIdQuery {
    public static int ORDER_ID_INDEX_CACHE_MAX_SIZE = 10000;
    public static final int CACHE_NUM_PER_MISS = 100;
    private static OrderIdIndexCache orderIdIndexCache;
    
    static {
        orderIdIndexCache = new OrderIdIndexCache(ORDER_ID_INDEX_CACHE_MAX_SIZE);
    }

    public static Order findByOrderId(long orderId, int index) {
        //System.out.println("==========:"+goodId + " index:" + index);
        Order order = new Order();
        try {
//            FileInputStream twoIndexFile = new FileInputStream(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_TWO_INDEXING_BY_ORDERID + index);
//            BufferedReader twoIndexBR = new BufferedReader(new InputStreamReader(twoIndexFile));

            //查找一级index缓存
            Long pos = orderIdIndexCache.get(orderId);
            if (pos == null) {
              //一级index缓存miss，从文件中读取，并cache若干一级index
                pos = firstIndexCacheMiss(orderId, index, true);
            }
            if (pos == null) {
                //对应order不存在
                return null;
            }

            File hashFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_INDEX_BY_ORDERID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");
            //System.out.println(pos);
            hashRaf.seek(Long.valueOf(pos));
            String orderContent = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
            //System.out.println(orderContent);

            //4.将字符串转成order对象集合
            String[] keyValues = orderContent.split("\t");
            for (int i = 0; i < keyValues.length; i++) {
                String[] strs = keyValues[i].split(":");
                KeyValue kv = new KeyValue();
                kv.setKey(strs[0]);
                kv.setValue(strs[1]);
                order.getKeyValues().put(strs[0], kv);
            }
            order.setId(orderId);
//            twoIndexBR.close();
            hashRaf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return order;
    }

    /**
     * @throws FileNotFoundException 
     * 
     */
    private static Long firstIndexCacheMiss(long orderId, int index, boolean cacheMoreIndex) throws IOException {
        //1.查找二·级索引
        long position = TwoIndexCache.findOrderIdOneIndexPosition(orderId, index);

        //2.查找一级索引
        File indexFile = new File(FileConstant.FIRST_DISK_PATH + FileConstant.FILE_ONE_INDEXING_BY_ORDERID + index);
        String str = null;
        RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
        indexRaf.seek(position);
        Long result = null;
        int cacheCount = cacheMoreIndex ? CACHE_NUM_PER_MISS : 0;
        int count = 0;
        String line = null;
        while ((line = indexRaf.readLine()) != null) {
            if (result == null && count++ < FileConstant.orderIdIndexRegionSizeMap.get(index)) {
                String[] keyValue = line.split(":");
                if (orderId == Long.valueOf(keyValue[0])) {
                    result = Long.valueOf(keyValue[1]);
                    orderIdIndexCache.put(orderId, result);
                }
            } else if (cacheCount-- > 0) {
                String[] keyValue = line.split(":");
                if (orderIdIndexCache.contains(Long.valueOf(keyValue[0]))) {
                    break;
                }
                orderIdIndexCache.put(Long.valueOf(keyValue[0]), Long.valueOf(keyValue[1]));
            } else {
                break;
            }
        }
        indexRaf.close();
        
        return result;
    }

    public static void main(String args[]) {

        //OrderIdIndexFile.generateGoodIdIndex();
        //findByOrderId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
