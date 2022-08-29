package com.aliyun.emr.rss.common.protocol;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.emr.rss.common.protocol.TransportMessages.PbStorageHint;

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

  public StorageInfo() {
  }

  public StorageInfo(Type type, boolean isFinal) {
    this.type = type;
    this.finalResult = isFinal;
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

  @Override
  public String toString() {
    return "StorageHint{" +
             "type=" + type +
             ", mountPoint='" + mountPoint + '\'' +
             ", finalResult=" + finalResult +
             '}';
  }

  public static PbStorageHint toPb(StorageInfo hint) {
    return PbStorageHint.newBuilder()
             .setType(hint.type.type)
             .setFinalResult(hint.finalResult)
             .setMountPoint(hint.mountPoint)
             .build();
  }

  public static StorageInfo fromPb(PbStorageHint pbHint) {
    return new StorageInfo(typesMap.get(pbHint.getType()), pbHint.getMountPoint(),
      pbHint.getFinalResult());
  }
}
