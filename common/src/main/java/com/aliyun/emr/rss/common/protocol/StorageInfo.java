package com.aliyun.emr.rss.common.protocol;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.emr.rss.common.protocol.TransportMessages.PbStorageInfo;

public class StorageInfo implements Serializable {
  public static String UNKNOWN_DISK = "UNKNOWN_DISK";

  public enum Type {
    MEMORY(0), HDD(1), SSD(2), HDFS(3), OSS(4);
    private final int type;

    Type(int type) {
      this.type = type;
    }

    public int getType() {
      return type;
    }
  }

  public static Map<Integer, Type> typesMap = new HashMap() {{
      for (Type type : Type.values()) {
        put(type.type, type);
      }
    }};

  // Default storage Type is MEMORY.
  private Type type = Type.MEMORY;
  private String mountPoint = UNKNOWN_DISK;
  // if a file is committed, field "finalResult" will be true
  private boolean finalResult = false;
  private String filePath;

  public StorageInfo() {
  }

  public StorageInfo(Type type, boolean isFinal, String filePath) {
    this.type = type;
    this.finalResult = isFinal;
    this.filePath = filePath;
  }

  public StorageInfo(String mountPoint) {
    this.mountPoint = mountPoint;
  }

  public StorageInfo(Type type, String mountPoint) {
    this.type = type;
    this.mountPoint = mountPoint;
  }

  public StorageInfo(Type type, String mountPoint, boolean finalResult) {
    this.type = type;
    this.mountPoint = mountPoint;
    this.finalResult = finalResult;
  }

  public StorageInfo(Type type, String mountPoint, boolean finalResult, String filePath) {
    this.type = type;
    this.mountPoint = mountPoint;
    this.finalResult = finalResult;
    this.filePath = filePath;
  }

  public boolean isFinalResult() {
    return finalResult;
  }

  public void setFinalResult(boolean finalResult) {
    this.finalResult = finalResult;
  }

  public String getMountPoint() {
    return mountPoint;
  }

  public void setMountPoint(String mountPoint) {
    this.mountPoint = mountPoint;
  }

  public Type getType() {
    return type;
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public String toString() {
    return "StorageInfo{" +
             "type=" + type +
             ", mountPoint='" + mountPoint + '\'' +
             ", finalResult=" + finalResult +
             ", filePath=" + filePath +
             '}';
  }

  public static PbStorageInfo toPb(StorageInfo storageInfo) {
    String filePath = storageInfo.getFilePath();
    PbStorageInfo.Builder builder = PbStorageInfo.newBuilder();
    builder.setType(storageInfo.type.type)
      .setFinalResult(storageInfo.finalResult)
      .setMountPoint(storageInfo.mountPoint);
    if (filePath != null) {
      builder.setFilePath(filePath);
    }
    return builder.build();
  }

  public static StorageInfo fromPb(PbStorageInfo pbStorageInfo) {
    return new StorageInfo(typesMap.get(pbStorageInfo.getType()), pbStorageInfo.getMountPoint(),
      pbStorageInfo.getFinalResult(), pbStorageInfo.getFilePath());
  }
}
