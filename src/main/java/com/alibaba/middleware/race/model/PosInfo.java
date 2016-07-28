/**
 * PosInfo.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.model;

/**
 * @author wangweiwei
 *
 */
public class PosInfo {
    public static final String POS_SPLITOR = "_";
    public static final String POS_SPLITOR_REX = "_";
    public int offset;
    public int length;
    
    public PosInfo(int offset, int length) {
        this.offset = offset;
        this.length = length;
    }
    
    @Override
    public String toString() {
        return String.valueOf(offset) + POS_SPLITOR + length;
    }

    public static PosInfo parseFromString(String posInfoString) {
        String[] split = posInfoString.split(POS_SPLITOR_REX);
        return new PosInfo(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
    }
}
