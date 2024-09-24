package org.apache.celeborn.common.container

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.rest.v1.model.ContainerInfo
import scala.collection.JavaConverters._

abstract class ContainerInfoProvider {

  def getContainerInfo(): ContainerInfo

}

object ContainerInfoProvider extends Logging {

  val DEFAULT_CONTAINER_NAME = "default_container_name"
  val DEFAULT_CONTAINER_DATA_CENTER = "default_container_data_center"
  val DEFAULT_CONTAINER_AVAILABILITY_ZONE = "default_container_availability_zone"
  val DEFAULT_CONTAINER_CLUSTER = "default_container_cluster"
  val DEFAULT_CONTAINER_TAGS = List.empty[String].asJava

  def instantiate(conf: CelebornConf): ContainerInfoProvider = {
    Utils.instantiate(conf.containerInfoProviderClass)
  }

}
