package com.alibaba.middleware.race.buyer;

import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.*;

/**
 * Created by jiangchao on 2016/7/15.
 */
public class BuyerIdIndexFile {

    private static Map<String, List<Long>> buyerIndex = new TreeMap<String, List<Long>>();

    public static void generateBuyerIdIndex() {

        for (int i = 0; i < FileConstant.FILE_NUMS; i++) {
            buyerIndex.clear();

            try {
                FileInputStream order_records = new FileInputStream(FileConstant.FILE_INDEX_BY_BUYERID + i);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                File file = new File(FileConstant.FILE_ONE_INDEXING_BY_BUYERID + i);
                FileWriter fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                File twoIndexfile = new File(FileConstant.FILE_TWO_INDEXING_BY_BUYERID + i);
                FileWriter twoIndexfw = new FileWriter(twoIndexfile);
                BufferedWriter twoIndexBW = new BufferedWriter(twoIndexfw);

                String str = null;
                long count = 0;

                while ((str = order_br.readLine()) != null) {
                    String buyerid = null;
                    String createtime = null;
                    String[] keyValues = str.split("\t");
                    for (int j = 0; j < keyValues.length; j++) {
                        String[] keyValue = keyValues[j].split(":");

                        if ("buyerid".equals(keyValue[0])) {
                            buyerid = keyValue[1];
                        } else if ("createtime".equals(keyValue[0])) {
                            createtime = keyValue[1];
                        }
                        if (buyerid != null && createtime != null) {
                            String newKey = buyerid + "_" + createtime;
                            if (!buyerIndex.containsKey(newKey)) {
                                buyerIndex.put(newKey, new ArrayList<Long>());
                            }
                            buyerIndex.get(newKey).add(count);
                            break;
                        }
                    }
                    count += str.getBytes().length + 2;
                }

                int twoIndexSize = (int) Math.sqrt(buyerIndex.size());
                FileConstant.buyerIdIndexRegionSizeMap.put(i, twoIndexSize);
                count = 0;
                long position = 0;
                Iterator iterator = buyerIndex.entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry entry = (Map.Entry) iterator.next();
                    String key = (String) entry.getKey();
                    List<Long> val = (List<Long>) entry.getValue();
                    String content = key + ":";
                    for (Long num : val) {
                        content = content + num + "|";
                    }
                    bufferedWriter.write(content);

                    if (count%twoIndexSize == 0) {
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
