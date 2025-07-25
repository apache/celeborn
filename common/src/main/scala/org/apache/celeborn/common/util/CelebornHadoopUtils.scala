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

package org.apache.celeborn.common.util

import java.io.{File, IOException}
import java.util
import java.util.HashSet
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{OSS_ACCESS_KEY, OSS_SECRET_KEY}
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.StorageInfo

object CelebornHadoopUtils extends Logging {
  private var logPrinted = new AtomicBoolean(false)
  private[celeborn] def newConfiguration(conf: CelebornConf): Configuration = {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.automatic.close", "false")
    if (conf.hdfsDir.nonEmpty) {
      val path = new Path(conf.hdfsDir)
      val scheme = path.toUri.getScheme
      val disableCacheName = String.format("fs.%s.impl.disable.cache", scheme)
      hadoopConf.set("dfs.replication", "2")
      hadoopConf.set(disableCacheName, "false")
      if (logPrinted.compareAndSet(false, true)) {
        logInfo(
          "Celeborn overrides some HDFS settings defined in Hadoop configuration files, " +
            s"including '$disableCacheName=false' and 'dfs.replication=2'. " +
            "It can be overridden again in Celeborn configuration with the additional " +
            "prefix 'celeborn.hadoop.', e.g. 'celeborn.hadoop.dfs.replication=3'")
      }
    }

    if (conf.s3Dir.nonEmpty) {
      if (conf.s3EndpointRegion.isEmpty) {
        throw new CelebornException("S3 storage is enabled but s3EndpointRegion is not set")
      }
      hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      hadoopConf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider," +
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider," +
          "com.amazonaws.auth.EnvironmentVariableCredentialsProvider," +
          "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")

      hadoopConf.set("fs.s3a.endpoint.region", conf.s3EndpointRegion)
    } else if (conf.ossDir.nonEmpty) {
      if (conf.ossAccessKey.isEmpty || conf.ossSecretKey.isEmpty || conf.ossEndpoint.isEmpty) {
        throw new CelebornException(
          "OSS storage is enabled but ossAccessKey, ossSecretKey, or ossEndpoint is not set")
      }
      if (conf.ossIgnoreCredentials) {
        hadoopConf.set("fs.oss.credentials.provider", "")
      }
      hadoopConf.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
      hadoopConf.set("fs.oss.accessKeyId", conf.ossAccessKey)
      hadoopConf.set("fs.oss.accessKeySecret", conf.ossSecretKey)
      hadoopConf.set("fs.oss.endpoint", conf.ossEndpoint)
    }
    appendSparkHadoopConfigs(conf, hadoopConf)
    hadoopConf
  }

  private def appendSparkHadoopConfigs(conf: CelebornConf, hadoopConf: Configuration): Unit = {
    // Copy any "celeborn.hadoop.foo=bar" celeborn properties into conf as "foo=bar"
    for ((key, value) <- conf.getAll if key.startsWith("celeborn.hadoop.")) {
      hadoopConf.set(key.substring("celeborn.hadoop.".length), value)
    }
  }

  def getHadoopFS(conf: CelebornConf): java.util.Map[StorageInfo.Type, FileSystem] = {
    val hadoopConf = newConfiguration(conf)
    initKerberos(conf, hadoopConf)
    val hadoopFs = new java.util.HashMap[StorageInfo.Type, FileSystem]()
    if (conf.hasHDFSStorage) {
      val hdfsDir = new Path(conf.hdfsDir)
      hadoopFs.put(StorageInfo.Type.HDFS, hdfsDir.getFileSystem(hadoopConf))
    }
    if (conf.hasS3Storage) {
      val s3Dir = new Path(conf.s3Dir)
      hadoopFs.put(StorageInfo.Type.S3, s3Dir.getFileSystem(hadoopConf))
    }
    if (conf.hasOssStorage) {
      val ossDir = new Path(conf.ossDir)
      hadoopFs.put(StorageInfo.Type.OSS, ossDir.getFileSystem(hadoopConf))
    }
    hadoopFs
  }

  def deleteDFSPathOrLogError(hadoopFs: FileSystem, path: Path, recursive: Boolean): Unit = {
    try {
      val startTime = System.currentTimeMillis()
      hadoopFs.delete(path, recursive)
      logInfo(
        s"Delete DFS ${path}(recursive=$recursive) costs " +
          Utils.msDurationToString(System.currentTimeMillis() - startTime))
    } catch {
      case e: IOException =>
        logError(s"Failed to delete DFS ${path}(recursive=$recursive) due to: ", e)
    }
  }

  def initKerberos(conf: CelebornConf, hadoopConf: Configuration): Unit = {
    UserGroupInformation.setConfiguration(hadoopConf)
    if ("kerberos".equals(hadoopConf.get("hadoop.security.authentication").toLowerCase)) {
      (conf.hdfsStorageKerberosPrincipal, conf.hdfsStorageKerberosKeytab) match {
        case (Some(principal), Some(keytab)) =>
          logInfo(
            s"Attempting to login to Kerberos using principal: $principal and keytab: $keytab")
          if (!new File(keytab).exists()) {
            throw new CelebornException(s"Keytab file: $keytab does not exist")
          }
          UserGroupInformation.loginUserFromKeytab(principal, keytab)
        case _ =>
          logInfo("Kerberos is enabled without principal and keytab supplied," +
            " assuming keytab is managed externally")
          UserGroupInformation.getCurrentUser()
      }
    }
  }
}
