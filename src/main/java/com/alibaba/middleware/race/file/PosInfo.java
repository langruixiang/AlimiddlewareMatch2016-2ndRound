/**
 * PosInfo.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.file;

/**
 * @author wangweiwei
 *
 */
public class PosInfo {
    public int offset;
    public int length;
    
    public PosInfo(int offset, int length) {
        this.offset = offset;
        this.length = length;
    }
}
