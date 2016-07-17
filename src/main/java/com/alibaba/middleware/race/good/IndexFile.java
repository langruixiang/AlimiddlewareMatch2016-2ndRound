package com.alibaba.middleware.race.good;

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

//    public static long bytes2Long(byte[] byteNum) {
//        long num = 0;
//        for (int ix = 0; ix < 8; ++ix) {
//            num <<= 8;
//            num |= (byteNum[ix] & 0xff);
//        }
//        return num;
//    }




}
