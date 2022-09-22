package com.aliyun.emr.rss.common.quota

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.UserIdentifier

class DefaultQuotaManager(conf: RssConf) extends QuotaManager(conf) {
  import QuotaManager._
  override def refresh(): Unit = {
    // Not support refresh
  }

  /**
   * Initialize user quota settings.
   */
  override def initialize(): Unit = {
    conf.getAll.foreach { case (key, value) =>
      if (QUOTA_REGEX.findPrefixOf(key).isDefined) {
        val QUOTA_REGEX(user, suffix) = key
        val userIdentifier = UserIdentifier(user)
        val quotaValue =
          try {
            value.toLong
          } catch {
            case e =>
              logError(
                s"Quota value of ${userIdentifier} should be a long value, incorrect setting : ${value}")
              -1
          }
        val quota = userQuotas.getOrDefault(userIdentifier, new Quota())
        quota.update(userIdentifier, suffix, quotaValue)
        userQuotas.put(userIdentifier, quota)
      }
    }
  }
}
