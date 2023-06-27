package org.apache.celeborn.common.util

import org.apache.hadoop.conf.Configuration

import org.apache.celeborn.common.CelebornConf

object CelebornHadoopUtils {
  private[celeborn] def newConfiguration(conf: CelebornConf): Configuration = {
    val hadoopConf = new Configuration()
    appendSparkHadoopConfigs(conf, hadoopConf)
    hadoopConf
  }

  private def appendSparkHadoopConfigs(conf: CelebornConf, hadoopConf: Configuration): Unit = {
    // Copy any "celeborn.hadoop.foo=bar" celeborn properties into conf as "foo=bar"
    for ((key, value) <- conf.getAll if key.startsWith("celeborn.hadoop.")) {
      hadoopConf.set(key.substring("celeborn.hadoop.".length), value)
    }
  }
}
