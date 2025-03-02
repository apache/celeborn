package org.apache.celeborn.service.deploy.worker.metrics

import org.apache.celeborn.common.metrics.sink.Sink
import org.apache.celeborn.service.deploy.worker.Worker

trait IWorkerMetricSink extends Sink {
  def init(worker: Worker): Unit
}
