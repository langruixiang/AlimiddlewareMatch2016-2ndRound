package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Order;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import java.io.*;
import java.util.*;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class IndexFile {

    private static Map<String, List<Long>> goodIndex = new TreeMap<String, List<Long>>();

    public static void generateGoodIdIndex() {

        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            goodIndex.clear();

            try {
                FileInputStream order_records = new FileInputStream(FileConstant.FILE_INDEX_BY_GOODID + i);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File file = new File(FileConstant.FILE_INDEXING_BY_GOODID + i);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                File twoIndexfile = new File(FileConstant.FILE_TWO_INDEXING_BY_GOODID + i);
                FileWriter twoIndexfw = new FileWriter(twoIndexfile);
                BufferedWriter twoIndexBW = new BufferedWriter(twoIndexfw);

                String str = null;
                long count = 0;
                String goodid = null;
                while ((str = order_br.readLine()) != null) {
                    String[] keyValues = str.split("\t");
                    for (int j = 0; j < keyValues.length; j++) {
                        String[] keyValue = keyValues[j].split(":");

                        if ("goodid".equals(keyValue[0])) {
                            goodid = keyValue[1];
                            if (!goodIndex.containsKey(goodid)) {
                                goodIndex.put(goodid, new ArrayList<Long>());
                            }
                            goodIndex.get(goodid).add(count);
                            break;
                        }
                    }
                    count += str.getBytes().length + 2;
                }

                int towIndexSize = (int) Math.sqrt(goodIndex.size());
                count = 0;
                long position = 0;
                Iterator iterator = goodIndex.entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry entry = (Map.Entry) iterator.next();
                    String key = (String) entry.getKey();
                    List<Long> val = (List<Long>) entry.getValue();
                    String content = key + ":";
                    for (Long num : val) {
                        content = content + num + "|";
                    }
                    bufferedWriter.write(content);

                    if (count%towIndexSize == 0) {
                        twoIndexBW.write(key+":");
                        twoIndexBW.write(String.valueOf(position));
                        twoIndexBW.newLine();
                    }
                    position += content.getBytes().length + 2;
                    bufferedWriter.newLine();

                    count++;
                }
                bufferedWriter.flush();
                bufferedWriter.close();
                twoIndexBW.flush();
                twoIndexBW.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static long bytes2Long(byte[] byteNum) {
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }

    public static List<Order> findByGoodId(String goodId, int index) {
        System.out.println("==========:"+goodId + " index:" + index);
        List<Order> orders = new ArrayList<Order>();
        try {
            FileInputStream twoIndexFile = null;
            twoIndexFile = new FileInputStream(FileConstant.FILE_TWO_INDEXING_BY_GOODID + index);
            BufferedReader twoIndexBR = new BufferedReader(new InputStreamReader(twoIndexFile));

            File hashFile = new File(FileConstant.FILE_INDEX_BY_GOODID + index);
            RandomAccessFile hashRaf = new RandomAccessFile(hashFile, "rw");

            File indexFile = new File(FileConstant.FILE_INDEXING_BY_GOODID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = 0;
            while ((str = twoIndexBR.readLine()) != null) {
                String[] keyValue = str.split(":");
                if (goodId.compareTo(keyValue[0]) < 0) {
                    System.out.println("--------"+keyValue[0]);
                   break;
                } else {
                    position = Long.valueOf(keyValue[1]);
                }
            }

            System.out.println(position);

            //2.查找一级索引
            indexRaf.seek(position);
            String oneIndex = null;
            while ((oneIndex = indexRaf.readLine()) != null) {
                String[] keyValue = oneIndex.split(":");
                if (goodId.equals(keyValue[0])) {
                    break;
                }
            }

            //3.按行读取内容
            String[] keyValue = oneIndex.split(":");
            System.out.println(keyValue[1]);
            String[] positions = keyValue[1].split("\\|");
            //System.out.println("======" + positions.length);
            for (String pos : positions) {
                System.out.println(pos);
                hashRaf.seek(Long.valueOf(pos));
                String orderContent = new String(hashRaf.readLine().getBytes("iso-8859-1"), "UTF-8");
                System.out.println(orderContent);

                //4.将字符串转成order对象集合
                Order order = new Order();
                String[] keyValues = orderContent.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] strs = keyValues[i].split(":");
                    KeyValue kv = new KeyValue();
                    kv.setKey(strs[0]);
                    kv.setValue(strs[1]);
                    order.getKeyValues().put(strs[0], kv);
                }
                order.setId(Long.valueOf(order.getKeyValues().get("orderid").getValue()));
                orders.add(order);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orders;
    }

    public static void main(String args[]) {

        //IndexFile.generateGoodIdIndex();
        findByGoodId("aliyun_2d7d53f7-fcf8-4095-ae6a-e54992ca79e5", 0);
    }
}
