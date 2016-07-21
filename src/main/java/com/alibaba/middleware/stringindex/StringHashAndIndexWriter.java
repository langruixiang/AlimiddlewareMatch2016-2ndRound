/**
 * StringHashAndIndexWriter.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.stringindex;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.middleware.race.util.FileUtil;

/**
 * @author wangweiwei
 *
 */
public class StringHashAndIndexWriter extends Thread{

    public static final int INIT_KEY_MAP_CAPACITY = 20; 

    private String regionId;
    
    private Map<String, Integer> keyMap;
    
    private LinkedBlockingQueue<StringIndex> indexQueue;
    
    private AtomicBoolean indexOver = new AtomicBoolean(false);
    
    private TreeMap<String, String> indexLineMap;
    
    private RandomAccessFile regionIndexFile;
//    private FileChannel regionIndexFileFC;
//    private MappedByteBuffer regionIndexFileMBB;
    
    private RandomAccessFile regionKeyValuesFile;
//    private FileChannel regionKeyValuesFileFC;
//    private MappedByteBuffer regionKeyValuesFileMBB;
    
    private String regionKeyMapFilePath;
    private String regionIndexFileName;
    private String regionKeyValuesFileName;

    public StringHashAndIndexWriter(String regionRootFolder, String regionId, String indexIdName) {
        this.regionId = regionId;
        indexQueue = new LinkedBlockingQueue<StringIndex>();
        keyMap = new HashMap<String, Integer>(INIT_KEY_MAP_CAPACITY);
        regionKeyMapFilePath = StringIndexRegion.getRegionKeyMapFilePath(regionRootFolder, regionId);
        regionIndexFileName = StringIndexRegion.getRegionIndexFilePath(regionRootFolder, this.regionId, indexIdName);
        regionKeyValuesFileName = StringIndexRegion.getRegionKeyValuesFilePath(regionRootFolder, this.regionId);
    }
    
    public void sendIndex(StringIndex stringIndex) {
        indexQueue.add(stringIndex);
    }
    
    public void sendIndexOverSignal() {
        indexOver.set(true);
    }

    @Override
    public void run() {
        this.indexLineMap = new TreeMap<String, String>();
        try {
            regionKeyValuesFile = new RandomAccessFile(regionKeyValuesFileName, "rw");
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        while (true) {
            StringIndex stringIndex = indexQueue.poll();
            if (stringIndex != null) {
                buildWithStringIndex(stringIndex);
            }
            if (indexOver.get() && indexQueue.isEmpty()) {
                try {
                    regionKeyValuesFile.close();
                    writeKeyMapFile();
                    writeIndexsToFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
    }
    
    /**
     * @throws IOException 
     * 
     */
    private void writeIndexsToFile() throws IOException {
        regionIndexFile = new RandomAccessFile(regionIndexFileName, "rw");
        Iterator<Entry<String, String>> iterator = indexLineMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            FileUtil.appendFixedBytesLineWithRandomAccessFile(regionIndexFile,
                    StringIndex.ENCODING, entry.getValue().toString(),
                    StringIndex.BYTES_OF_INDEX_FILE_LINE);
        }
        regionIndexFile.close();
    }

    /**
     * @param stringIndex
     */
    private void buildWithStringIndex(StringIndex stringIndex) {
        Map<String, String> keyValueMap = stringIndex.getKeyValueMap();
        for (Entry<String, String> entry : keyValueMap.entrySet()) {
            if (!keyMap.containsKey(entry.getKey())) {
                keyMap.put(entry.getKey(), keyMap.size());
            }
        }
        ArrayList<String> keyPosInfo = new ArrayList<String>(keyMap.size());
        for (int i = 0; i < keyMap.size(); ++i) {
            keyPosInfo.add("0");
        }
        try {
            // write regionKeyValuesFile
            boolean isFirstEntry = true;
            for (Entry<String, String> entry : keyValueMap.entrySet()) {
                String key = entry.getKey();
                if (isFirstEntry) {
                    isFirstEntry = false;
                    regionKeyValuesFile.write(key.concat(":").getBytes(StringIndex.ENCODING));
                } else {
                    regionKeyValuesFile.write("\t".concat(key.concat(":")).getBytes(StringIndex.ENCODING));
                }
                
                Integer keyIndex = keyMap.get(entry.getKey());
                long offset = regionKeyValuesFile.length();
                byte[] bytes = entry.getValue().getBytes(StringIndex.ENCODING);
                int length = bytes.length;
                regionKeyValuesFile.write(bytes);
                keyPosInfo.set(keyIndex,
                        String.valueOf(offset).concat(StringIndex.POS_SPLITOR)
                                .concat(String.valueOf(length)));
            }
            regionKeyValuesFile.write("\n".getBytes(StringIndex.ENCODING));
        } catch (IOException e) {
            e.printStackTrace();
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

    private void writeKeyMapFile() {
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(regionKeyMapFilePath), StringIndex.ENCODING));
            for (Entry<String, Integer> entry : keyMap.entrySet()) {
                writer.write(entry.getKey().concat(":").concat(entry.getValue().toString()).concat("\n"));
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
}
