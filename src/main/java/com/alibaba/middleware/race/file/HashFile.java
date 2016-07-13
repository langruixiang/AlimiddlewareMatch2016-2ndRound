package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.model.Good;
import com.alibaba.middleware.race.model.Order;

import java.io.*;
import java.util.Collection;

/**
 * Created by jiangchao on 2016/7/12.
 */
public class HashFile {

    public static void generateOrderIdHashFile(Collection<String> orderFiles, Collection<String> buyerFiles,
                                               Collection<String> goodFiles, Collection<String> storeFolders, int nums) throws IOException, InterruptedException{

        BufferedWriter[] bufferedWriters = new BufferedWriter[nums];

        for (int i = 0; i < nums; i++) {
            File file = new File(FileConstant.FILE_INDEX + i);
            FileWriter fw = new FileWriter(file);
            bufferedWriters[i] = new BufferedWriter(fw);
        }

        for (String orderFile : orderFiles) {
            FileInputStream order_records = new FileInputStream(orderFile);
            BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

            String str = null;
            Long orderid = null;
            int hashFileIndex;
            while ((str = order_br.readLine()) != null) {
                String[] keyValues = str.split("\t");
                for (int i = 0; i < keyValues.length; i++) {
                    String[] keyValue = keyValues[i].split(":");
                    if ("orderid".equals(keyValue[0])) {
                        orderid = Long.valueOf(keyValue[1]);
                        hashFileIndex = (int) (orderid % nums);
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

    }
}
