/**
 * RandomAccessFileUtil.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

/**
 * @author wangweiwei
 *
 */
public class RandomAccessFileUtil {

    public static String readLine(RandomAccessFile raf, long offset) throws IOException {
        raf.seek(offset);
        long pos = offset;
        long length = raf.length();
        LinkedList<byte[]> bytesList = new LinkedList<byte[]>();
        int lineLength = 0;
        while (pos < length) {
            byte[] buffer = new byte[1024];
            pos += raf.read(buffer);
            int eofIndex = -1;
            for(int i = 0; i < buffer.length; ++i) {
                if (buffer[i] == '\n' || buffer[i] == '\r') {
                    eofIndex = i;
                    break;
                }
            }
            if (eofIndex > 0) {
                lineLength += eofIndex;
                bytesList.add(subBytes(buffer, 0, eofIndex));
                break;
            } else if (eofIndex < 0) {
                lineLength += buffer.length;
                bytesList.add(buffer);
            } else {
                break;
            }
        }
        if (lineLength > 0) {
            byte[] lineBuffer = new byte[lineLength];
            int tmpPos = 0;
            for (byte[] b : bytesList) {
                System.arraycopy(b, 0, lineBuffer, tmpPos, b.length);
                tmpPos += b.length;
            }
            return new String(lineBuffer);
        } else if (pos < length) {
            return "";
        } else {
            return null;
        }
    }

    public static byte[] subBytes(byte[] src, int beginIndex, int length)
    {
        byte[] ret = new byte[length];
        System.arraycopy(src, beginIndex, ret, 0, length);
        return ret;
    } 
}
