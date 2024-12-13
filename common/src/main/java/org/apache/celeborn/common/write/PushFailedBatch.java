package org.apache.celeborn.common.write;

import java.io.Serializable;

import com.google.common.base.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class PushFailedBatch implements Serializable {

  private int mapId;
  private int attemptId;
  private int batchId;
  private int epoch;
  private int reduceId;

  public int getMapId() {
    return mapId;
  }

  public void setMapId(int mapId) {
    this.mapId = mapId;
  }

  public int getAttemptId() {
    return attemptId;
  }

  public void setAttemptId(int attemptId) {
    this.attemptId = attemptId;
  }

  public int getBatchId() {
    return batchId;
  }

  public void setBatchId(int batchId) {
    this.batchId = batchId;
  }

  public int getReduceId() {
    return reduceId;
  }

  public void setReduceId(int reduceId) {
    this.reduceId = reduceId;
  }

  public int getEpoch() {
    return epoch;
  }

  public void setEpoch(int epoch) {
    this.epoch = epoch;
  }

  public PushFailedBatch(int mapId, int attemptId, int batchId, int reduceId, int epoch) {
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.batchId = batchId;
    this.reduceId = reduceId;
    this.epoch = epoch;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PushFailedBatch)) {
      return false;
    }
    PushFailedBatch o = (PushFailedBatch) other;
    return super.equals(o)
        && mapId == o.mapId
        && attemptId == o.attemptId
        && batchId == o.batchId
        && reduceId == o.reduceId
        && epoch == o.epoch;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), mapId, attemptId, batchId, reduceId, epoch);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("mapId", mapId)
        .append("attemptId", attemptId)
        .append("batchId", batchId)
        .append("reduceId", reduceId)
        .append("epoch", epoch)
        .toString();
  }
}
