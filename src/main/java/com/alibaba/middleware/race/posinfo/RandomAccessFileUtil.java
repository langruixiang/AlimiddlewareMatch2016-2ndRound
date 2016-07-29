/**
 * RandomAccessFileUtil.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.posinfo;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.alibaba.middleware.race.model.PosInfo;

/**
 * @author wangweiwei
 *
 */
public class RandomAccessFileUtil {

    public static String readLine(RandomAccessFile raf, long offset) throws IOException {
        StringBuffer sb = new StringBuffer();
        byte[] buffer = new byte[768];//与编码字节有关，不要随意改变768！！！
        raf.seek(offset);
        long pos = offset;
        long length = raf.length();
        while (pos < length) {
            pos += raf.read(buffer);
            String tmpString = new String(buffer);
            int changeLineCharacterIdx = tmpString.indexOf('\n');
            if (changeLineCharacterIdx >= 0) {
                sb.append(tmpString.substring(0, changeLineCharacterIdx));
                return sb.toString();
            } else {
                changeLineCharacterIdx = tmpString.indexOf('\r');
                if (changeLineCharacterIdx > 0) {
                    sb.append(tmpString.substring(0, changeLineCharacterIdx));
                    return sb.toString();
                } else{
                    sb.append(tmpString);
                }
            }
        }
        return sb.toString();
    }
}
