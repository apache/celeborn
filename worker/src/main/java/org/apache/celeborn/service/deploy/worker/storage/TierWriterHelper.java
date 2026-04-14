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

package org.apache.celeborn.service.deploy.worker.storage;

import org.apache.hadoop.fs.FileSystem;

import org.apache.celeborn.reflect.DynConstructors;
import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler;

public class TierWriterHelper {

  public static AutoCloseable getS3MultipartUploadHandlerSharedState(
      FileSystem hadoopFs, String bucketName, int maxRetryies, int baseDelay, int maxBackoff) {
    return (AutoCloseable)
        DynConstructors.builder()
            .impl(
                "org.apache.celeborn.S3MultipartUploadHandler$S3MultipartUploadHandlerSharedState",
                FileSystem.class,
                String.class,
                Integer.class,
                Integer.class,
                Integer.class)
            .build()
            .newInstance(hadoopFs, bucketName, maxRetryies, baseDelay, maxBackoff);
  }

  public static MultipartUploadHandler getS3MultipartUploadHandler(
      AutoCloseable sharedState, String key) {
    return (MultipartUploadHandler)
        DynConstructors.builder()
            .impl("org.apache.celeborn.S3MultipartUploadHandler", AutoCloseable.class, String.class)
            .build()
            .newInstance(sharedState, key);
  }

  public static MultipartUploadHandler getOssMultipartUploadHandler(
      String ossEndpoint, String bucketName, String ossAccessKey, String ossSecretKey, String key) {
    return (MultipartUploadHandler)
        DynConstructors.builder()
            .impl(
                "org.apache.celeborn.OssMultipartUploadHandler",
                String.class,
                String.class,
                String.class,
                String.class,
                String.class)
            .build()
            .newInstance(ossEndpoint, bucketName, ossAccessKey, ossSecretKey, key);
  }
}
