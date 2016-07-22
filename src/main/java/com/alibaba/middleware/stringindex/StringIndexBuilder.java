/**
 * StringIndexBuilder.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.middleware.race.util.FileUtil;

/**
 * @author wangweiwei
 *
 */
public class StringIndexBuilder extends Thread {

    private String regionRootFolder;
    private CountDownLatch countDownLatch;
    private String indexIdName;
    private int regionId;
    private TreeMap<String, String> indexLineMap;
    
    public static final int INIT_KEY_MAP_CAPACITY = 20; 
    private Map<String, Integer> keyMap;
    
    private RandomAccessFile regionIndexFile;
    
    private BufferedReader regionKeyValuesFileBR;
    
    public StringIndexBuilder(String regionRootFolder,
            int regionId, String indexIdName, CountDownLatch countDownLatch) {
        if (!regionRootFolder.endsWith("/")) {
            regionRootFolder = regionRootFolder.concat("/");
        }
        this.regionRootFolder = regionRootFolder;
        this.countDownLatch = countDownLatch;
        this.indexIdName = indexIdName;
        this.regionId = regionId;
    }

    public void build() {
        try {
            keyMap = FileUtil.readSIHashMapFromFile(StringIndexRegion
                    .getRegionKeyMapFilePath(regionRootFolder, regionId),
                    INIT_KEY_MAP_CAPACITY);
            FileInputStream fis = new FileInputStream(StringIndexRegion.getRegionKeyValuesFilePath(
                    regionRootFolder, regionId));
            InputStreamReader isr = new InputStreamReader(fis, StringIndex.ENCODING); 
            regionKeyValuesFileBR = new BufferedReader(isr); 
            String line = null;
            long startByteNo = 0;
            while ((line = regionKeyValuesFileBR.readLine()) != null) {
                buildWithLine(line, startByteNo);
                startByteNo += line.concat("\n").getBytes(StringIndex.ENCODING).length;
            }
            regionKeyValuesFileBR.close();
            writeIndexsToFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @throws IOException 
     * 
     */
    private void writeIndexsToFile() throws IOException {
        regionIndexFile = new RandomAccessFile(
                StringIndexRegion.getRegionIndexFilePath(regionRootFolder,
                        regionId, indexIdName), "rw");
        Iterator<Entry<String, String>> iterator = indexLineMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            FileUtil.appendFixedBytesLineWithRandomAccessFile(regionIndexFile,
                    StringIndex.ENCODING, entry.getValue().toString(),
                    StringIndex.BYTES_OF_INDEX_FILE_LINE);
        }
        
        regionIndexFile.close();
    }
    
    private void buildWithLine(String line, long startByteNo) throws IOException {
        StringIndex stringIndex = new StringIndex();
        ArrayList<String> keyPosInfo = new ArrayList<String>(keyMap.size());
        for (int i = 0; i < keyMap.size(); ++i) {
            keyPosInfo.add("0");
        }
        String[] keyValues = line.split("\t");
        long offset = startByteNo;
        long length = 0;
        for (int i = 0; i < keyValues.length; i++) {
            String[] keyValue = keyValues[i].split(":");
            if (indexIdName.equals(keyValue[0])) {
                stringIndex.setIndexIdName(keyValue[0]);
                stringIndex.setIndexId(keyValue[1]);
            }
            stringIndex.addKeyValue(keyValue[0], keyValue[1]);
            offset += keyValue[0].concat(":").getBytes(StringIndex.ENCODING).length;
            length = keyValue[1].getBytes(StringIndex.ENCODING).length;
            Integer keyIndex = keyMap.get(keyValue[0]);
            keyPosInfo.set(keyIndex,
                    String.valueOf(offset).concat(StringIndex.POS_SPLITOR)
                            .concat(String.valueOf(length)));
            offset += (length + "\t".getBytes(StringIndex.ENCODING).length);
        }
        
        // write regionIndex
        StringBuilder indexLineSb = new StringBuilder();
        indexLineSb.append(stringIndex.getIndexId())
                .append(StringIndex.INDEX_SPLITOR).append(keyPosInfo.get(0));
        for (int i = 1; i < keyPosInfo.size(); ++i) {
            indexLineSb.append(StringIndex.KEY_SPLITOR).append(
                    keyPosInfo.get(i));
        }
        indexLineMap.put(stringIndex.getIndexId(), indexLineSb.toString());
    }

    @Override
    public void run(){
        if (StringIndexRegion.isRegionExist(regionRootFolder, regionId)) {
            this.indexLineMap = new TreeMap<String, String>();
            build();
        }
        System.out.println("StringIndexBuilder build end~");
        countDownLatch.countDown();
    }
}
