package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.Collection;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class GoodHashFile {

    //读取所有订单文件，按照订单号hash到多个小文件中
    public static void generateGoodHashFile(Collection<String> orderFiles, Collection<String> buyerFiles,
                                               Collection<String> goodFiles, Collection<String> storeFolders, int nums) {

        try {
            BufferedWriter[] bufferedWriters = new BufferedWriter[nums];

            for (int i = 0; i < nums; i++) {
                File file = new File(FileConstant.FILE_GOOD_HASH + i);
                FileWriter fw = null;
                fw = new FileWriter(file);
                bufferedWriters[i] = new BufferedWriter(fw);
            }

            for (String orderFile : orderFiles) {
                FileInputStream good_records = new FileInputStream(orderFile);
                BufferedReader good_br = new BufferedReader(new InputStreamReader(good_records));

                String str = null;
                long goodid = 0;
                int hashFileIndex;
                while ((str = good_br.readLine()) != null) {
                    String[] keyValues = str.split("\t");
                    for (int i = 0; i < keyValues.length; i++) {
                        String[] keyValue = keyValues[i].split(":");
                        if ("goodid".equals(keyValue[0])) {
                            goodid = keyValue[1].hashCode();
                            hashFileIndex = (int) (Math.abs(goodid) % nums);
                            bufferedWriters[hashFileIndex].write(str);
                            bufferedWriters[hashFileIndex].newLine();
                            break;
                        }
                    }
                }
            }

            for (int i = 0; i < nums; i++) {
                bufferedWriters[i].close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
