package com.microsoft.nao.infra;

import com.sun.jna.ptr.PointerByReference;

public class CelebornMeasureMetric5D {
  private PointerByReference hMetric;

  private CelebornMeasureMetric5D(PointerByReference hMetric) {
    this.hMetric = hMetric;
  }

  public static CelebornMeasureMetric5D create(
      String monitoringAccount,
      String metricNamespace,
      String metricName,
      String dimensionName1,
      String dimensionName2,
      String dimensionName3,
      String dimensionName4,
      String dimensionName5) {
    return create(monitoringAccount, metricNamespace, metricName,
        dimensionName1, dimensionName2, dimensionName3, dimensionName4,
        dimensionName5, false);
  }

  public static CelebornMeasureMetric5D create(
      String monitoringAccount,
      String metricNamespace,
      String metricName,
      String dimensionName1,
      String dimensionName2,
      String dimensionName3,
      String dimensionName4,
      String dimensionName5,
      boolean addDefaultDimension) {
    PointerByReference hMetric = new PointerByReference();
    String[] dimensions = new String[5];
    dimensions[0] = dimensionName1;
    dimensions[1] = dimensionName2;
    dimensions[2] = dimensionName3;
    dimensions[3] = dimensionName4;
    dimensions[4] = dimensionName5;
    long rc = CelebornIfxMetricsInterface.INSTANCE.CreateIfxMeasureMetric(
        hMetric,
        monitoringAccount,
        metricNamespace,
        metricName,
        5,
        dimensions,
        addDefaultDimension);
    if (rc >= 0) {
      return new CelebornMeasureMetric5D(hMetric);
    }

    return null;
  }

  public long LogValue(
      long rawData,
      String dimensionValue1,
      String dimensionValue2,
      String dimensionValue3,
      String dimensionValue4,
      String dimensionValue5) {
    String[] dimensions = new String[5];
    dimensions[0] = dimensionValue1;
    dimensions[1] = dimensionValue2;
    dimensions[2] = dimensionValue3;
    dimensions[3] = dimensionValue4;
    dimensions[4] = dimensionValue5;

    return CelebornIfxMetricsInterface.INSTANCE.SetIfxMeasureMetric(this.hMetric.getValue(), rawData, 5, dimensions);
  }

  public long LogValue(
      long timestampUtc,
      long rawData,
      String dimensionValue1,
      String dimensionValue2,
      String dimensionValue3,
      String dimensionValue4,
      String dimensionValue5) {
    String[] dimensions = new String[5];
    dimensions[0] = dimensionValue1;
    dimensions[1] = dimensionValue2;
    dimensions[2] = dimensionValue3;
    dimensions[3] = dimensionValue4;
    dimensions[4] = dimensionValue5;

    return CelebornIfxMetricsInterface.INSTANCE.SetIfxMeasureMetricWithTimestamp(this.hMetric.getValue(), timestampUtc, rawData, 5, dimensions);
  }


  public double LogValue(
      double rawData,
      String dimensionValue1,
      String dimensionValue2,
      String dimensionValue3,
      String dimensionValue4,
      String dimensionValue5) {
    String[] dimensions = new String[5];
    dimensions[0] = dimensionValue1;
    dimensions[1] = dimensionValue2;
    dimensions[2] = dimensionValue3;
    dimensions[3] = dimensionValue4;
    dimensions[4] = dimensionValue5;

    return CelebornIfxMetricsInterface.INSTANCE.SetIfxMeasureMetric(this.hMetric.getValue(), rawData, 5, dimensions);
  }
}

