/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.master.scale

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder, KubernetesClientException}
import io.fabric8.kubernetes.client.dsl.Resource
import io.fabric8.kubernetes.client.dsl.base.{PatchContext, PatchType, ResourceDefinitionContext}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.WorkerStats

class OpenKruiseScaleManager(conf: CelebornConf) extends BaseK8sScaleManager(conf) {

  private val namespace = conf.get(CelebornConf.SCALE_KUBERNETES_NAMESPACE)
  private val statefulSetName = conf.get(CelebornConf.SCALE_KUBERNETES_WORKER_STATEFULSET).get
  private lazy val client = createClient()
  private lazy val calculator = new SpecWorkerCntCalculator(conf, configService)
  private var observedResourceVersion = Option.empty[String]

  protected def createClient(): KubernetesClient = new KubernetesClientBuilder().build()

  override protected def calculateScaleTarget(
      registeredWorkerCnt: Int,
      specWorkerCnt: Int,
      workerStats: Seq[WorkerStats],
      exclWorkerCnt: Int,
      unaccountedExclWorkerCnt: Int): ScaleTarget = {
    val result = calculator.cal(SpecWorkerCntCalculator.Input(
      registeredWorkerCnt,
      specWorkerCnt,
      workerStats,
      exclWorkerCnt,
      unaccountedExclWorkerCnt))
    ScaleTarget(result.targetSpecWorkerCnt, result.accountedExclWorkerCnt)
  }

  private def statefulSet: Resource[GenericKubernetesResource] = {
    client.genericKubernetesResources(OpenKruiseScaleManager.resourceDefinition)
      .inNamespace(namespace).withName(statefulSetName)
  }

  override protected def getCurrentClusterInfo(): ClusterInfo = {
    val clusterInfo = readClusterInfo(statefulSet.get())
    observedResourceVersion = Option(clusterInfo.platformVersion)
    clusterInfo
  }

  private def readClusterInfo(resource: GenericKubernetesResource): ClusterInfo = {
    val specWorkerCnt = resource.get[Number]("spec", "replicas").intValue()
    val reservedWorkerIndices =
      Option(resource.get[java.util.List[Number]]("spec", "reserveOrdinals"))
        .map(_.asScala.map(_.intValue()).toSeq).getOrElse(Seq.empty)
    ClusterInfo(
      specWorkerCnt,
      reservedWorkerIndices,
      isComplete(resource, resource.getMetadata.getGeneration),
      resource.getMetadata.getUid,
      resource.getMetadata.getResourceVersion)
  }

  override protected def doScaleOperation(operation: ScaleOperation): String = {
    val version = observedResourceVersion.getOrElse {
      throw new IllegalStateException("A StatefulSet resource version is required to scale workers")
    }
    val resourceVersion = client.getKubernetesSerialization.asJson(version)
    val reserved = operation.reservedWorkerIndices.mkString(",")
    val patch =
      s"""{"metadata":{"resourceVersion":$resourceVersion},"spec":{"replicas":${operation.specWorkerCnt},"reserveOrdinals":[$reserved]}}"""
    val updated = statefulSet.patch(PatchContext.of(PatchType.JSON_MERGE), patch)
    if (!readClusterInfo(updated).matchesSpec(operation)) {
      throw new IllegalStateException(
        "The StatefulSet response did not contain the requested worker spec")
    }
    updated.getMetadata.getGeneration.toString
  }

  override protected def checkOperation(operationId: String): OperationResult = {
    val complete = isComplete(statefulSet.get(), operationId.toLong)
    OperationResult(complete, complete)
  }

  override protected def isRejectedPlatformUpdate(error: Exception): Boolean = error match {
    case e: KubernetesClientException =>
      OpenKruiseScaleManager.rejectedPatchCodes.contains(e.getCode)
    case _ => super.isRejectedPlatformUpdate(error)
  }

  private def isComplete(resource: GenericKubernetesResource, generation: Long): Boolean = {
    Option(resource.get[Number]("status", "observedGeneration")).exists(
      _.longValue() >= generation) &&
    Option(resource.get[Number]("status", "replicas")).exists(
      _.intValue() == resource.get[Number]("spec", "replicas").intValue())
  }

  override def stop(): Unit = {
    super.stop()
    client.close()
  }
}

object OpenKruiseScaleManager {
  private val rejectedPatchCodes = Set(400, 401, 403, 404, 405, 409, 410, 413, 415, 422)

  private val resourceDefinition = new ResourceDefinitionContext.Builder()
    .withGroup("apps.kruise.io")
    .withVersion("v1beta1")
    .withPlural("statefulsets")
    .withKind("StatefulSet")
    .withNamespaced(true)
    .build()
}
