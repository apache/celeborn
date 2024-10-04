package org.apache.celeborn.service.deploy.master.http.api.v1

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAMasterMetaManager
import org.apache.celeborn.service.deploy.master.http.api.MasterHttpResourceUtils.ensureMasterHAEnabled

import java.io.ByteArrayOutputStream
import javax.ws.rs.{Consumes, POST, Path, Produces}
import javax.ws.rs.core.{MediaType, Response}

@Tag(name = "Ratis")
@Consumes(Array(MediaType.APPLICATION_JSON))
class RatisLocalResource extends ApiRequestContext with Logging  {
  private def master = httpService.asInstanceOf[Master]
  private def ratisServer = master.statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[Response]))),
    description = "Generate a new-raft-meta.conf file.")
  @POST
  @Path("/raft_meta_conf")
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  def localRaftMetaConf(): Response = ensureMasterHAEnabled(master) {
    Response.ok(ratisServer.getGroupInfo().getLogInfoProto().writeTo(new ByteArrayOutputStream()))
      .header("Content-Disposition", "attachment; filename=\"new-raft-meta.conf\"")
      .build()
  }
}
