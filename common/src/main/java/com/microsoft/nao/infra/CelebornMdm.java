package com.microsoft.nao.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class CelebornMdm {
  private static final Logger log = LoggerFactory.getLogger(CelebornMdm.class);
  private String account;
  private String namespace;

  public CelebornMdm(String account, String namespace){
    this.account = account;
    this.namespace = namespace;
  }

  public void ReportMetric3D(String metricName, String[] dimensions, String[] dimensionValue, long metricValue) {
    CelebornMeasureMetric3D metric = CelebornMeasureMetric3D.create(
        this.account,
        this.namespace,
        metricName,
        dimensions[0],
        dimensions[1],
        dimensions[2]);

    metric.LogValue(metricValue, dimensionValue[0], dimensionValue[1], dimensionValue[2]);
  }

  public void ReportMetric5D(String metricName, String[] dimensions,
      String[] dimensionValue, long metricValue) {
    CelebornMeasureMetric5D metric = CelebornMeasureMetric5D.create(
        this.account,
        this.namespace,
        metricName,
        dimensions[0],
        dimensions[1],
        dimensions[2],
        dimensions[3],
        dimensions[4]);

    long statusCode = metric.LogValue(metricValue, dimensionValue[0],
        dimensionValue[1],
        dimensionValue[2], dimensionValue[3], dimensionValue[4]);

    if (statusCode != 0) {
      log.error(
          "ReportMetric5D for metric {}:{} with dimensions {}:{} failed with statusCode {}.",
          metricName, metricValue, Arrays.toString(dimensions),
          Arrays.toString(dimensionValue), statusCode);
    }
  }

  public void ReportMetric3D(String metricName, String[] dimensions, String[] dimensionValue, double metricValue) {
    CelebornMeasureMetric3D metric = CelebornMeasureMetric3D.create(
        this.account,
        this.namespace,
        metricName,
        dimensions[0],
        dimensions[1],
        dimensions[2]);
    metric.LogValue(metricValue, dimensionValue[0], dimensionValue[1], dimensionValue[2]);
  }
}
