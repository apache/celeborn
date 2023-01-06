package org.apache.celeborn.integration.test

import io.fabric8.kubernetes.client.{Config, KubernetesClientBuilder}
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.celeborn.common.internal.Logging

trait WithMiniKube extends Logging {

  lazy val kubernetesClient: KubernetesClient =
    new KubernetesClientBuilder().withConfig(Config.autoConfigure("minikube"))
      .build()
}
