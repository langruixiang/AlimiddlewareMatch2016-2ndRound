package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.Collection;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class BuyerHashFile {

    //读取所有订单文件，按照订单号hash到多个小文件中
    public static void generateBuyerHashFile(Collection<String> orderFiles, Collection<String> buyerFiles,
                                               Collection<String> goodFiles, Collection<String> storeFolders, int nums) {

        try {
            BufferedWriter[] bufferedWriters = new BufferedWriter[nums];

            for (int i = 0; i < nums; i++) {
                File file = new File(FileConstant.FILE_BUYER_HASH + i);
                FileWriter fw = null;
                fw = new FileWriter(file);
                bufferedWriters[i] = new BufferedWriter(fw);
            }

            for (String buyerFile : buyerFiles) {
                FileInputStream buyer_records = new FileInputStream(buyerFile);
                BufferedReader buyer_br = new BufferedReader(new InputStreamReader(buyer_records));

                String str = null;
                long buyerid = 0;
                int hashFileIndex;
                while ((str = buyer_br.readLine()) != null) {
                    String[] keyValues = str.split("\t");
                    for (int i = 0; i < keyValues.length; i++) {
                        String[] keyValue = keyValues[i].split(":");
                        if ("buyerid".equals(keyValue[0])) {
                            buyerid = keyValue[1].hashCode();
                            hashFileIndex = (int) (Math.abs(buyerid) % nums);
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
