package com.alibaba.middleware.race.model;

/**
 * Created by jiangchao on 2016/7/31.
 */
public class FilePosition {

    private String fileName;
    private long position;

    public FilePosition(String fileName, long position) {
        this.fileName = fileName;
        this.position = position;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }
}
