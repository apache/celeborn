package org.apache.celeborn.common.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import com.codahale.metrics.MetricRegistry
import com.microsoft.nao.infra.MdmReporter

class MdmSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  import MdmSink._

  val monitoringAccount = Option(property.getProperty(MDM_KEY_MONITORING_ACCOUNT)) match {
    case Some(s) => s
    case None => ""
  }

  val metricNamespace = Option(property.getProperty(MDM_KEY_METRIC_NAMESPACE)) match {
    case Some(s) => s
    case None => ""
  }

  val pollPeriod = Option(property.getProperty(MDM_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => MDM_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(MDM_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(MDM_DEFAULT_UNIT)
  }

  val metricRegex: Option[Pattern] = Option(property.getProperty(MDM_KEY_METRIC_PATTERN)) match {
    case Some(s) => Some(Pattern.compile(s))
    case None => None
  }

  val reporter: MdmReporter = MdmReporter.forRegistry(registry)
    .overrideMonitoringAccount(this.monitoringAccount)
    .overrideMetricNamespace(this.metricNamespace)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .filter((name, _) => metricRegex.forall(_.matcher(name).matches()))
    .build()

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}

object MdmSink {
  val MDM_DEFAULT_PERIOD = 1
  val MDM_DEFAULT_UNIT = "MINUTES"

  val MDM_KEY_MONITORING_ACCOUNT = "monitoringAccount"
  val MDM_KEY_METRIC_NAMESPACE = "metricNamespace"
  val MDM_KEY_PERIOD = "period"
  val MDM_KEY_UNIT = "unit"
  val MDM_KEY_METRIC_PATTERN = "metricPattern"
}
