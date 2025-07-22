package com.microsoft.nao.infra;

public class Mdm {
  private String account;
  private String namespace;

  public Mdm(String account, String namespace){
    this.account = account;
    this.namespace = namespace;
  }

  public void ReportMetric3D(String metricName, String[] dimensions, String[] dimensionValue, long metricValue) {
    MeasureMetric3D metric = MeasureMetric3D.create(
        this.account,
        this.namespace,
        metricName,
        dimensions[0],
        dimensions[1],
        dimensions[2]);

    metric.LogValue(metricValue, dimensionValue[0], dimensionValue[1], dimensionValue[2]);
  }

  public void ReportMetric3D(String metricName, String[] dimensions, String[] dimensionValue, double metricValue) {
    MeasureMetric3D metric = MeasureMetric3D.create(
        this.account,
        this.namespace,
        metricName,
        dimensions[0],
        dimensions[1],
        dimensions[2]);
    metric.LogValue(metricValue, dimensionValue[0], dimensionValue[1], dimensionValue[2]);
  }
}
