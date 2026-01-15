package com.microsoft.nao.infra;

import com.sun.jna.ptr.PointerByReference;

public class CelebornMeasureMetric3D {
  private PointerByReference hMetric;

  private CelebornMeasureMetric3D(PointerByReference hMetric) {
    this.hMetric = hMetric;
  }

  public static CelebornMeasureMetric3D create(
      String monitoringAccount,
      String metricNamespace,
      String metricName,
      String dimensionName1,
      String dimensionName2,
      String dimensionName3) {
    return create(monitoringAccount, metricNamespace, metricName, dimensionName1, dimensionName2, dimensionName3, false);
  }

  public static CelebornMeasureMetric3D create(
      String monitoringAccount,
      String metricNamespace,
      String metricName,
      String dimensionName1,
      String dimensionName2,
      String dimensionName3,
      boolean addDefaultDimension) {
    PointerByReference hMetric = new PointerByReference();
    String[] dimensions = new String[3];
    dimensions[0] = dimensionName1;
    dimensions[1] = dimensionName2;
    dimensions[2] = dimensionName3;
    long rc = CelebornIfxMetricsInterface.INSTANCE.CreateIfxMeasureMetric(
        hMetric,
        monitoringAccount,
        metricNamespace,
        metricName,
        3,
        dimensions,
        addDefaultDimension);
    if (rc >= 0) {
      return new CelebornMeasureMetric3D(hMetric);
    }

    return null;
  }

  public long LogValue(
      long rawData,
      String dimensionValue1,
      String dimensionValue2,
      String dimensionValue3) {
    String[] dimensions = new String[3];
    dimensions[0] = dimensionValue1;
    dimensions[1] = dimensionValue2;
    dimensions[2] = dimensionValue3;

    return CelebornIfxMetricsInterface.INSTANCE.SetIfxMeasureMetric(this.hMetric.getValue(), rawData, 3, dimensions);
  }

  public long LogValue(
      long timestampUtc,
      long rawData,
      String dimensionValue1,
      String dimensionValue2,
      String dimensionValue3) {
    String[] dimensions = new String[3];
    dimensions[0] = dimensionValue1;
    dimensions[1] = dimensionValue2;
    dimensions[2] = dimensionValue3;

    return CelebornIfxMetricsInterface.INSTANCE.SetIfxMeasureMetricWithTimestamp(this.hMetric.getValue(), timestampUtc, rawData, 3, dimensions);
  }


  public double LogValue(
      double rawData,
      String dimensionValue1,
      String dimensionValue2,
      String dimensionValue3) {
    String[] dimensions = new String[3];
    dimensions[0] = dimensionValue1;
    dimensions[1] = dimensionValue2;
    dimensions[2] = dimensionValue3;

    return CelebornIfxMetricsInterface.INSTANCE.SetIfxMeasureMetric(this.hMetric.getValue(), rawData, 3, dimensions);
  }
}
