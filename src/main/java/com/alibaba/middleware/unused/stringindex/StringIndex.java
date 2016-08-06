package com.alibaba.middleware.unused.stringindex;

import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class StringIndex {
    public static final StringIndex NULL = new StringIndex();

    //UTF_8("UTF-8"), GB2312("GB2312"), GBK("GBK");
    public static final String ENCODING = "UTF-8";//TODO

    public static final String INDEX_SPLITOR = ":";
    public static final String KEY_SPLITOR = "|";
    public static final String POS_SPLITOR = "_";
    public static final String INDEX_SPLITOR_REX = ":";
    public static final String KEY_SPLITOR_REX = "\\|";
    public static final String POS_SPLITOR_REX = "_";

    private String indexIdName;

    private String indexId;
    
    private String[] keysPos;

    private Map<String, String> keyValueMap = null;
    
    public String getIndexId() {
        return indexId;
    }

    public void setIndexId(String indexId) {
        this.indexId = indexId;
    }

    public String getIndexIdName() {
        return indexIdName;
    }

    public void setIndexIdName(String indexIdName) {
        this.indexIdName = indexIdName;
    }

    public void setKeysPos(String[] keyPos) {
        this.keysPos = keyPos;
    }

    public String[] getKeysPos() {
        return keysPos;
    }

    public Map<String, String> getKeyValueMap() {
        return keyValueMap;
    }

    public void setKeyValueMap(Map<String, String> keyValueMap) {
        this.keyValueMap = keyValueMap;
    }

    public static StringIndex parseFromLine(String line) {
        if (line == null || line.isEmpty()) {
            return StringIndex.NULL;
        }
        StringIndex stringIndex = new StringIndex();
        String[] splitOfLine = line.split(StringIndex.INDEX_SPLITOR_REX);
        stringIndex.setIndexId(splitOfLine[0]);
        String[] parsedKeysPos = splitOfLine[1].split(StringIndex.KEY_SPLITOR_REX);
        stringIndex.setKeysPos(parsedKeysPos);
        return stringIndex;
    }

    public String getValueByKey(String key) {
        if (keyValueMap == null) {
            return null;
        }
        return keyValueMap.get(key);
    }

    /**
     * @param keys
     */
    public void loadKeyValue(int keyIndex, String key, MappedByteBuffer keyFileMBB) {
        if (keyValueMap == null) {
            keyValueMap = new HashMap<String, String>();
        }
        if (keyIndex < keysPos.length) {
            String posInfo = keysPos[keyIndex];
            String[] posDetail = posInfo.split(POS_SPLITOR_REX);
            if (posDetail.length == 2) {
                int offset = Integer.parseInt(posDetail[0]);
                int length = Integer.parseInt(posDetail[1]);
                keyFileMBB.position(offset);
                byte[] buffer = new byte[length];
                keyFileMBB.get(buffer);
                try {
                    keyValueMap.put(key, new String(buffer, ENCODING));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            } else {
                keyValueMap.put(key, null);
            }
        } else {
            keyValueMap.put(key, null);
        }
    }

    public LinkedList<String> getNeedLoadKeys(Collection<String> keys) {
        if (keyValueMap == null) {
            keyValueMap = new HashMap<String, String>();
        }
        LinkedList<String> ret= new LinkedList<String>();
        for (String key : keys) {
            if (!keyValueMap.containsKey(key)) {
                ret.add(key);
            }
        }
        return ret;
    }

    public void addKeyValue(String key, String value) {
        if (keyValueMap == null) {
            keyValueMap = new HashMap<String, String>();
        }
        
        keyValueMap.put(key, value);
    }

}
