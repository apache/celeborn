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

package org.apache.celeborn;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler;

public class S3MultipartUploadHandler implements MultipartUploadHandler {

  private static final Logger logger = LoggerFactory.getLogger(S3MultipartUploadHandler.class);

  private String uploadId;
  private final String key;

  private final S3MultipartUploadHandlerSharedState sharedState;

  public static class S3MultipartUploadHandlerSharedState implements AutoCloseable {

    private final S3Client s3Client;
    private final String bucketName;
    private final int s3MultiplePartUploadMaxRetries;
    private final int baseDelay;
    private final int maxBackoff;

    public S3MultipartUploadHandlerSharedState(
        FileSystem hadoopFs,
        String bucketName,
        Integer s3MultiplePartUploadMaxRetries,
        Integer baseDelay,
        Integer maxBackoff)
        throws IOException, URISyntaxException {
      this.bucketName = bucketName;
      this.s3MultiplePartUploadMaxRetries = s3MultiplePartUploadMaxRetries;
      this.baseDelay = baseDelay;
      this.maxBackoff = maxBackoff;
      Configuration conf = hadoopFs.getConf();
      URI binding = new URI(String.format("s3a://%s", bucketName));

      // maxAttempts counts the first try; the v1 client's maxErrorRetry counted retries only.
      ClientOverrideConfiguration overrideConfig =
          ClientOverrideConfiguration.builder()
              .retryStrategy(
                  AwsRetryStrategy.standardRetryStrategy()
                      .toBuilder()
                      .maxAttempts(s3MultiplePartUploadMaxRetries + 1)
                      .backoffStrategy(
                          BackoffStrategy.exponentialDelay(
                              Duration.ofMillis(baseDelay), Duration.ofMillis(maxBackoff)))
                      .build())
              .build();
      S3ClientBuilder builder =
          S3Client.builder()
              .credentialsProvider(getCredentialsProvider(binding, conf))
              .overrideConfiguration(overrideConfig);
      String region = conf.get(Constants.AWS_REGION);
      if (!StringUtils.isEmpty(region)) {
        builder = builder.region(Region.of(region));
      }
      // for MinIO
      String endpoint = conf.get(Constants.ENDPOINT);
      if (!StringUtils.isEmpty(endpoint)) {
        if (!endpoint.contains("://")) {
          endpoint = "https://" + endpoint;
        }
        builder =
            builder
                .endpointOverride(URI.create(endpoint))
                .forcePathStyle(conf.getBoolean(Constants.PATH_STYLE_ACCESS, false));
      }
      this.s3Client = builder.build();
    }

    @Override
    public void close() {
      if (s3Client != null) {
        s3Client.close();
      }
    }
  }

  public S3MultipartUploadHandler(AutoCloseable sharedState, String key) {
    this.sharedState = (S3MultipartUploadHandlerSharedState) sharedState;
    this.key = key;
  }

  @Override
  public void startUpload() {
    CreateMultipartUploadRequest initRequest =
        CreateMultipartUploadRequest.builder().bucket(sharedState.bucketName).key(key).build();
    this.uploadId = sharedState.s3Client.createMultipartUpload(initRequest).uploadId();
  }

  @Override
  public void putPart(InputStream inputStream, Integer partNumber, Boolean finalFlush)
      throws IOException {
    try (InputStream inStream = inputStream) {
      int partSize = inStream.available();
      if (partSize == 0) {
        logger.debug(
            "key {} uploadId {} part size is 0 for part number {} finalFlush {}",
            key,
            uploadId,
            partNumber,
            finalFlush);
        return;
      }
      UploadPartRequest uploadRequest =
          UploadPartRequest.builder()
              .bucket(sharedState.bucketName)
              .key(key)
              .uploadId(uploadId)
              .partNumber(partNumber)
              .contentLength((long) partSize)
              .build();
      sharedState.s3Client.uploadPart(
          uploadRequest, RequestBody.fromInputStream(inStream, partSize));
      logger.debug(
          "key {} uploadId {} part number {} uploaded with size {} finalFlush {}",
          key,
          uploadId,
          partNumber,
          partSize,
          finalFlush);
    } catch (RuntimeException | IOException e) {
      logger.error("Failed to upload part", e);
      throw e;
    }
  }

  @Override
  public void complete() {
    List<CompletedPart> completedParts = new ArrayList<>();
    ListPartsRequest listPartsRequest =
        ListPartsRequest.builder()
            .bucket(sharedState.bucketName)
            .key(key)
            .uploadId(uploadId)
            .build();
    for (Part part : sharedState.s3Client.listPartsPaginator(listPartsRequest).parts()) {
      completedParts.add(
          CompletedPart.builder().partNumber(part.partNumber()).eTag(part.eTag()).build());
    }
    if (completedParts.isEmpty()) {
      logger.debug(
          "bucket {} key {} uploadId {} has no parts uploaded, aborting upload",
          sharedState.bucketName,
          key,
          uploadId);
      abort();
      logger.debug(
          "bucket {} key {} upload completed with size {}", sharedState.bucketName, key, 0);
      return;
    }

    CompleteMultipartUploadRequest compRequest =
        CompleteMultipartUploadRequest.builder()
            .bucket(sharedState.bucketName)
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
            .build();
    CompleteMultipartUploadResponse compResult = null;
    for (int attempt = 1; attempt <= sharedState.s3MultiplePartUploadMaxRetries; attempt++) {
      try {
        compResult = sharedState.s3Client.completeMultipartUpload(compRequest);
        break;
      } catch (SdkException e) {
        if (attempt == sharedState.s3MultiplePartUploadMaxRetries || !isRetryable(e)) {
          logger.error(
              "bucket {} key {} uploadId {} upload failed to complete, will not retry",
              sharedState.bucketName,
              key,
              uploadId,
              e);
          throw e;
        }

        long backoffTime =
            Math.min(
                sharedState.maxBackoff, sharedState.baseDelay * (long) Math.pow(2, attempt - 1));
        try {
          logger.warn(
              "bucket {} key {} uploadId {} upload failed to complete, will retry ({}/{})",
              sharedState.bucketName,
              key,
              uploadId,
              attempt,
              sharedState.s3MultiplePartUploadMaxRetries,
              e);
          Thread.sleep(backoffTime);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
    logger.debug(
        "bucket {} key {} uploadId {} upload completed location is in {} ",
        sharedState.bucketName,
        key,
        uploadId,
        compResult.location());
  }

  @Override
  public void abort() {
    AbortMultipartUploadRequest abortRequest =
        AbortMultipartUploadRequest.builder()
            .bucket(sharedState.bucketName)
            .key(key)
            .uploadId(uploadId)
            .build();
    sharedState.s3Client.abortMultipartUpload(abortRequest);
  }

  @Override
  public void close() {}

  /** Client-side errors, throttling and 5xx are retried, as the v1 default retry condition did. */
  static boolean isRetryable(SdkException e) {
    if (e instanceof AwsServiceException) {
      AwsServiceException ase = (AwsServiceException) e;
      return ase.isThrottlingException() || ase.statusCode() >= 500;
    }
    return e instanceof SdkClientException || e.retryable();
  }

  static AWSCredentialProviderList getCredentialsProvider(URI binding, Configuration conf)
      throws IOException {
    return CredentialProviderListFactory.createAWSCredentialProviderList(binding, conf);
  }
}
