package com.aliyun.emr.rss.common.protocol;

public enum PartitionType {
  REDUCE_PARTITION(0),
  MAP_PARTITION(1),
  MAPGROUP_REDUCE_PARTITION(2);

  private final int value;

  PartitionType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
