package com.microsoft.nao.infra;

import com.codahale.metrics.*;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import java.util.Properties;
import java.io.FileInputStream;


public class CelebornMdmReporter extends ScheduledReporter {

  private static final Logger logger = LoggerFactory.getLogger(
      CelebornMdmReporter.class);
  private static final String[] dimensions = {"CelebornCluster", "Role",
      "MachineName", "Environment", "Cluster"};

  private static final String[] threeDimensions = {"CelebornCluster",
      "MachineName", "Environment"};

  private static String UNKNOWN = "UNKNOWN";

  private final CelebornMdm mdm;
  private String celebornCluster;
  private String role = "Server";
  private String machineName;
  private String environmentName;
  private String cluster;
  /**
   * Returns a new {@link Builder} for {@link CelebornMdmReporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link CelebornMdmReporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder for {@link CelebornMdmReporter} instances.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private String monitoringAccount;
    private String metricNamespace;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;

      this.monitoringAccount = "mtp-celeborn";
      this.metricNamespace = "celeborn";
    }

    public Builder overrideMonitoringAccount(String monitoringAccount) {
      if (monitoringAccount != null && !monitoringAccount.equals("")) {
        this.monitoringAccount = monitoringAccount;
      }

      return this;
    }

    public Builder overrideMetricNamespace(String metricNamespace) {
      if (metricNamespace != null && !metricNamespace.equals("")) {
        this.metricNamespace = metricNamespace;
      }

      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Builds a {@link CelebornMdmReporter} with the given properties.
     *
     * @return a {@link CelebornMdmReporter}
     */
    public CelebornMdmReporter build() {
      logger.info("Init MdmReport, monitoringAccount {}, metricNamespace {}.",
          this.monitoringAccount, this.metricNamespace);
      CelebornMdm mdm = new CelebornMdm(this.monitoringAccount, this.metricNamespace);
      return new CelebornMdmReporter(
          this.registry,
          mdm,
          rateUnit,
          durationUnit,
          filter);
    }
  }

  private CelebornMdmReporter(
      MetricRegistry registry,
      CelebornMdm mdm,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      MetricFilter filter) {
    super(registry, "mdm-reporter", filter, rateUnit, durationUnit);
    this.mdm = mdm;
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    printGauges(gauges);
    printCounters(counters);
  }

  private String getCelebornCluster() {
    if (celebornCluster == null || celebornCluster.equals(UNKNOWN)) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(System.getenv(
            "CELEBORN_CONF_DIR") + File.separator  + "metrics.properties")) {
          props.load(fis);
          // Read values
          celebornCluster = props.getProperty("CelebornCluster", UNKNOWN);
        } catch (Throwable e) {
          logger.warn("Failed to celeborn cluster name. Set to UNKNOWN", e);
          celebornCluster = UNKNOWN;
        }

    }
    return celebornCluster;
  }

  private String getEnvironment() {
    if (environmentName == null || environmentName.equals(UNKNOWN)) {
      if (!Strings.isNullOrEmpty(System.getenv("Environment"))) {
        environmentName = System.getenv("Environment");
      } else {
        environmentName = UNKNOWN;
      }
    }
    return environmentName;
  }

  private String getCluster() {
    if (cluster == null || cluster.equals(UNKNOWN)) {
      if (!Strings.isNullOrEmpty(System.getenv("Cluster"))) {
        cluster = System.getenv("Cluster");
      } else {
        cluster = UNKNOWN;
      }
    }
    return cluster;
  }

  private String getMachineName() {
    if (machineName == null || machineName.equals(UNKNOWN)) {
      try {
        this.machineName = InetAddress.getLocalHost().getHostName();
      } catch (Exception e) {
        logger.warn("Failed to get machine name. Set to UNKNOWN", e);
        machineName = UNKNOWN;
      }
    }

    return machineName;
  }

  private void printGauges(SortedMap<String, Gauge> metrics) {
    for (Map.Entry<String, Gauge> entry : metrics.entrySet()) {
      Gauge gauge = entry.getValue();
      long value = formatToLong(gauge.getValue(), entry.getKey());
      printMetric(entry.getKey(), value);
    }
  }

  private void printCounters(SortedMap<String, Counter> metrics){
    for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
      Counter counter = entry.getValue();
      printMetric(entry.getKey(), counter.getCount());
    }
  }

  private void printMetric(String key, long value){
    String metricName = MetricRegistry.name("", key);
    String[] dimensionsValue = {getCelebornCluster(), role, getMachineName(),
        getEnvironment(), getCluster() };
    this.mdm.ReportMetric5D(metricName, dimensions, dimensionsValue, value);

    String roleValue = role;
    Map<String, String> labels = extractLabels(metricName);
    if (labels.size() <= 1) {
      return;
    } else if (labels.size() > 3) {
      logger.info(
          "Extra 2 dimensions are supported at most now, {} has {} " + "labels.",
          metricName, labels.size());
      return;
    } else if (labels.size() == 3) {
      roleValue = labels.remove("role");
    }

    // labels.size() is 2 now
    String[] newDimensions = new String[dimensions.length];
    String[] newDimensionsValue = new String[dimensions.length];
    int braceIndex = metricName.lastIndexOf('{');
    String newMetricName = braceIndex >= 0 ? metricName.substring(0, braceIndex) : metricName;
    if (!newMetricName.contains("worker") && !newMetricName.contains(
        "master") && roleValue != null) {
      newMetricName = roleValue + "." + newMetricName;
    }

    String[] threeDimensionsValue = { getCelebornCluster(),
        getMachineName(), getEnvironment() };
    System.arraycopy(threeDimensions, 0, newDimensions, 0,
        threeDimensions.length);
    System.arraycopy(threeDimensionsValue, 0, newDimensionsValue, 0,
        threeDimensionsValue.length);

    int i = threeDimensions.length;
    for (Map.Entry<String, String> labelEntry : labels.entrySet()) {
      newDimensions[i] = labelEntry.getKey();
      newDimensionsValue[i] = labelEntry.getValue();
      i++;
    }
    this.mdm.ReportMetric5D(newMetricName, newDimensions,
        newDimensionsValue, value);
  }

  private Map<String, String> extractLabels(String metricName) {
    int start = metricName.lastIndexOf('{');
    int end = metricName.lastIndexOf('}');
    Map<String, String> labels = new HashMap<>();
    if (start >= 0 && end > start) {
      String labelString = metricName.substring(start + 1, end);
      String[] parts = labelString.split(",");
      for (String part : parts) {
        String[] kv = part.split("=", 2);
        if (kv.length == 2) {
          String val = kv[1];
          if (val.startsWith("\"")) {
            val = val.substring(1);
          }
          if (val.endsWith("\"")) {
            val = val.substring(0, val.length() - 1);
          }
          labels.put(kv[0], val);
        }
      }
    }
    return labels;
  }

  // MDM only accepts long, multiple 100 for percentage value.
  private long formatToLong(Object o, String metricName){
    if(metricName.endsWith(".usage") && o instanceof Double){
      Double usage = ((Double) o ) * 100;
      return usage.longValue();
    }

    return formatToLong(o);
  }

  private long formatToLong(Object o) {
    if (o instanceof Float) {
      return ((Float) o).longValue();
    } else if (o instanceof Double) {
      return ((Double) o).longValue();
    } else if (o instanceof Byte) {
      return ((Byte) o).longValue();
    } else if (o instanceof Short) {
      return ((Short) o).longValue();
    } else if (o instanceof Integer) {
      return ((Integer) o).longValue();
    } else if (o instanceof Long) {
      return ((Long) o).longValue();
    }

    return -1;
  }
}
