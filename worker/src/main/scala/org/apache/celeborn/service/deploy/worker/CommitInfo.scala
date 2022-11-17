package org.apache.celeborn.service.deploy.worker

import org.apache.celeborn.common.protocol.message.ControlMessages.CommitFilesResponse

class CommitInfo(var response: CommitFilesResponse, var status: Int)

object CommitInfo {
  val COMMIT_NOTSTARTED: Int = 0
  val COMMIT_INPROCESS: Int = 1
  val COMMIT_FINISHED: Int = 2
}
