package com.microsoft.nao.infra;

import com.codahale.metrics.*;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import java.util.Properties;
import java.io.FileInputStream;


public class MdmReporter extends ScheduledReporter {

  private static final Logger logger = LoggerFactory.getLogger(MdmReporter.class);
  private static final String[] dimensions = {"CelebornCluster", "Role",
      "MachineName", "Environment", "Cluster"};
  private static String UNKNOWN = "UNKNOWN";

  private final Mdm mdm;
  private String celebornCluster;
  private String role = "Server";
  private String machineName;
  private String environmentName;
  private String cluster;
  /**
   * Returns a new {@link Builder} for {@link MdmReporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link MdmReporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder for {@link MdmReporter} instances.
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
     * Builds a {@link MdmReporter} with the given properties.
     *
     * @return a {@link MdmReporter}
     */
    public MdmReporter build() {
      logger.info("Init MdmReport, monitoringAccount {}, metricNamespace {}.",
          this.monitoringAccount, this.metricNamespace);
      Mdm mdm = new Mdm(this.monitoringAccount, this.metricNamespace);
      return new MdmReporter(
          this.registry,
          mdm,
          rateUnit,
          durationUnit,
          filter);
    }
  }

  private MdmReporter(
      MetricRegistry registry,
      Mdm mdm,
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
