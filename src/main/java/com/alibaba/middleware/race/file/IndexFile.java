package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.cache.PageCache;
import com.alibaba.middleware.race.constant.FileConstant;
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
            FileInputStream order_records = null;
            FileInputStream goodid_index = null;
            try {
                order_records = new FileInputStream(FileConstant.FILE_INDEX_BY_GOODID + i);
                BufferedReader order_br = new BufferedReader(new InputStreamReader(order_records));

                goodid_index = new FileInputStream(FileConstant.FILE_INDEXING_BY_GOODID + i);
                BufferedReader goodidIndex_br = new BufferedReader(new InputStreamReader(goodid_index));

                File file = new File(FileConstant.FILE_INDEXING_BY_GOODID + i);
                FileWriter fw = null;
                fw = new FileWriter(file);
                BufferedWriter bufferedWriter = new BufferedWriter(fw);

                File twoIndexfile = new File(FileConstant.FILE_TWO_INDEXING_BY_GOODID + i);
                FileWriter twoIndexfw = null;
                twoIndexfw = new FileWriter(twoIndexfile);
                BufferedWriter twoIndexBW = new BufferedWriter(twoIndexfw);

                FileOutputStream indexLinePositionfile = new FileOutputStream(FileConstant.FILE_INDEX_LINE_POSITION + i);
                DataOutputStream lineIndexDO = null;
                lineIndexDO = new DataOutputStream(indexLinePositionfile);


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
                long lineByteNums = 0;
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

//                    lineIndexDO.writeLong(lineByteNums);
//                    //lineIndexDO.newLine();
//                    lineByteNums += content.getBytes().length + 2;

                    count++;
                }
//                lineIndexDO.writeLong(lineByteNums);
//                lineIndexDO.flush();
//                lineIndexDO.close();
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

    public static void testByte() {
        FileInputStream goodid_index = null;
        try {
            goodid_index = new FileInputStream(FileConstant.FILE_INDEX_LINE_POSITION + 0);
            DataInputStream goodidIndex_br = new DataInputStream(goodid_index);
            String str = null;
            byte[] bytes = new byte[16];
            //System.out.println(goodidIndex_br.read(bytes, 0, 8));
            //goodidIndex_br.readFully();
            System.out.println(bytes2Long(bytes));
            while ((str = goodidIndex_br.readLine()) != null) {
                System.out.println(str);
                System.out.println("==========:" + str.getBytes().length);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static  byte[] getLineBytes(RandomAccessFile raf, long lineNumber) {
        //File newFile=new File(FileConstant.FILE_INDEX_LINE_POSITION + 0);
        //RandomAccessFile raf= null;
        byte[] bytes = new byte[8];
        try {
            //raf = new RandomAccessFile(newFile, "rw");
            raf.seek(8 * lineNumber);
            raf.read(bytes,0,8);
            //String str = new String(bytes, "utf-8");
            System.out.println("==========:" + bytes2Long(bytes));
            //raf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }

    public static void testRandomRead(String filePath, long lineNumber) {
        File newFile=new File(FileConstant.FILE_INDEX_LINE_POSITION + 0);
        RandomAccessFile raf= null;
        try {
            raf = new RandomAccessFile(newFile, "rw");
            raf.seek(16);
            byte[] bytes = new byte[8];
            raf.read(bytes,0,8);
            //String str = new String(bytes, "utf-8");
            System.out.println("==========:" + bytes2Long(bytes));
            raf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void findByGoodId(String goodId, int index) {


        try {
            FileInputStream twoIndexFile = null;
            twoIndexFile = new FileInputStream(FileConstant.FILE_TWO_INDEXING_BY_GOODID + index);
            BufferedReader twoIndexBR = new BufferedReader(new InputStreamReader(twoIndexFile));

            File newFile = new File(FileConstant.FILE_INDEX_LINE_POSITION + index);
            RandomAccessFile raf = new RandomAccessFile(newFile, "rw");

            File indexFile = new File(FileConstant.FILE_INDEXING_BY_GOODID + index);
            RandomAccessFile indexRaf = new RandomAccessFile(indexFile, "rw");
            String str = null;

            //1.查找二·级索引
            long position = 0;
            while ((str = twoIndexBR.readLine()) != null) {
                String[] keyValues = str.split(":");
                if (goodId.compareTo(keyValues[0]) < 0) {
                   break;
                } else {
                    position = Long.valueOf(keyValues[1]);
                }
            }

            System.out.println(position);

            //2.查找一级索引

            //2.1先读取对应的index_line_position文件找到对应行的偏移位置
//            byte[] begin = getLineBytes(raf, position);
//            byte[] end = getLineBytes(raf, position+1);
//            long beginposition = bytes2Long(begin);
//            int length = (int) (bytes2Long(end) - beginposition - 2);
//            raf.close();
            indexRaf.seek(position);
            String oneIndex = indexRaf.readLine();
            System.out.println(oneIndex);

            //2.2获取一级索引对应值
//            indexRaf.seek(beginposition);
//            byte[] content = new byte[length];
//            //indexRaf.read(content, 0, length);
//            String contentStr = indexRaf.readLine();
//            //String contentStr = new String(content);
//            System.out.println(contentStr);

            //3.按行读取内容


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String args[]) {

        //IndexFile.generateGoodIdIndex();
        //testByte();
        //testRandomRead(null, 0);

        findByGoodId("aliyun_891ed350-2313-4997-8d72-f0ad0200c0c5", 0);
    }
}
