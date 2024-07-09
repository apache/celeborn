package org.apache.celeborn.service.deploy.worker.storage;

import org.apache.celeborn.reflect.DynConstructors;
import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler;

public class TierWriterHelper {
  public static MultipartUploadHandler getS3MultipartUploadHandler(
      String bucketName,
      String s3AccessKey,
      String s3SecretKey,
      String s3EndpointRegion,
      String key,
      int maxRetryies) {
    return (MultipartUploadHandler)
        DynConstructors.builder()
            .impl(
                "org.apache.celeborn.S3MultipartUploadHandler",
                String.class,
                String.class,
                String.class,
                String.class,
                String.class,
                Integer.class)
            .build()
            .newInstance(bucketName, s3AccessKey, s3SecretKey, s3EndpointRegion, key, maxRetryies);
  }
}
