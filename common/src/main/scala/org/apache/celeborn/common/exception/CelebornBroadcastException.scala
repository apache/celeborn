package org.apache.celeborn.common.exception

class CelebornBroadcastException(message: String, cause: Throwable)
  extends CelebornIOException(message, cause) {
  def this(message: String) = this(message, null)
}
