package org.apache.spark.shuffle.rss

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle

class RssShuffleHandle[K, V, C](
  val newAppId: String,
  val rssMetaServiceHost: String,
  val rssMetaServicePort: Int,
  shuffleId: Int,
  val numMappers: Int,
  dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle(shuffleId, dependency) {
}
