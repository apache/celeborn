package org.apache.celeborn.common.container
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.rest.v1.model.ContainerInfo

class DefaultContainerInfoProvider extends ContainerInfoProvider {

  override def getContainerInfo(): ContainerInfo = {
    new ContainerInfo()
      .containerName(ContainerInfoProvider.DEFAULT_CONTAINER_NAME)
      .containerHostName(Utils.getHostName(false))
      .containerAddress(Utils.getHostName(true))
      .containerDataCenter(ContainerInfoProvider.DEFAULT_CONTAINER_DATA_CENTER)
      .containerAvailabilityZone(ContainerInfoProvider.DEFAULT_CONTAINER_AVAILABILITY_ZONE)
      .containerUser(sys.env("user.name"))
      .containerCluster(ContainerInfoProvider.DEFAULT_CONTAINER_CLUSTER)
      .containerTags(ContainerInfoProvider.DEFAULT_CONTAINER_TAGS)
  }
}
