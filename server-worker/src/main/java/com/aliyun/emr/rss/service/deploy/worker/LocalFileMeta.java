package com.aliyun.emr.rss.service.deploy.worker;

import java.io.File;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LocalFileMeta {
  String shuffleKey;
  String fileName;
  File file;
  long bytesFlushed = 0;
  ArrayList<Long> chunkOffsets = new ArrayList<>();

  @JsonCreator
  public LocalFileMeta(
     @JsonProperty("shuffleKey") String shuffleKey,
     @JsonProperty("fileName") String fileName,
     @JsonProperty("file") File file,
     @JsonProperty("bytesFlushed") long bytesFlushed,
     @JsonProperty("chunkOffsets") ArrayList<Long> chunkOffsets) {
    this.shuffleKey = shuffleKey;
    this.fileName = fileName;
    this.file = file;
    this.bytesFlushed = bytesFlushed;
    this.chunkOffsets = chunkOffsets;
  }

  public LocalFileMeta(String shuffleKey, String fileName, File file) {
    this.shuffleKey = shuffleKey;
    this.fileName = fileName;
    this.file = file;
    chunkOffsets.add(0L);
  }

  String getAbsolutePath() {
    return file.getAbsolutePath();
  }

  long getFileLength() {
    return bytesFlushed;
  }

  long length() {
    return file.length();
  }

  public String getShuffleKey() {
    return shuffleKey;
  }

  public void setShuffleKey(String shuffleKey) {
    this.shuffleKey = shuffleKey;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  public long getBytesFlushed() {
    return bytesFlushed;
  }

  public void setBytesFlushed(long bytesFlushed) {
    this.bytesFlushed = bytesFlushed;
  }

  public ArrayList<Long> getChunkOffsets() {
    return chunkOffsets;
  }

  public void setChunkOffsets(ArrayList<Long> chunkOffsets) {
    this.chunkOffsets = chunkOffsets;
  }

  @Override
  public String toString() {
    return "LocalFileMeta{" +
        "shuffleKey='" + shuffleKey + '\'' +
        ", fileName='" + fileName + '\'' +
        ", file=" + file +
        ", bytesFlushed=" + bytesFlushed +
        ", chunkOffsets=" + chunkOffsets +
        '}';
  }
}
