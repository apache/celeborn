package org.apache.spark.shuffle.rss

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle

import com.aliyun.emr.rss.common.protocol.message.ControlMessages.UserIdentifier

class RssShuffleHandle[K, V, C](
    val newAppId: String,
    val rssMetaServiceHost: String,
    val rssMetaServicePort: Int,
    val userIdentifier: UserIdentifier,
    shuffleId: Int,
    numMappers: Int,
    dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle(shuffleId, numMappers, dependency)
