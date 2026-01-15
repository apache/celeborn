package com.microsoft.nao.infra;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public interface CelebornIfxMetricsInterface extends Library {

  CelebornIfxMetricsInterface INSTANCE = (CelebornIfxMetricsInterface) Native.loadLibrary("IfxMetrics", CelebornIfxMetricsInterface.class);


  long CreateIfxMeasureMetric(PointerByReference hMetric,
      String monitoringAccount,
      String metricNamespace,
      String metricName,
      int countDimension,
      String[] listDimensionNames,
      boolean addDefaultDimension);

  long SetIfxMeasureMetric(Pointer hMetric, long rawData, int countDimension, String[] dimensionValues);

  long SetIfxMeasureMetricWithTimestamp(Pointer hMetric, long timestampUtc, long rawData, int countDimension, String[] dimensionValues);

  double SetIfxMeasureMetric(Pointer hMetric, double rawData, int countDimension, String[] dimensionValues);
}

