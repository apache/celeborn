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
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler;

public class S3MultipartUploadHandler implements MultipartUploadHandler {

  private static final Logger logger = LoggerFactory.getLogger(S3MultipartUploadHandler.class);

  private String uploadId;
  private final String key;

  private final S3MultipartUploadHandlerSharedState sharedState;

  public static class S3MultipartUploadHandlerSharedState implements AutoCloseable {

    private final AmazonS3 s3Client;
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

      RetryPolicy retryPolicy =
          new RetryPolicy(
              PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
              new PredefinedBackoffStrategies.SDKDefaultBackoffStrategy(
                  baseDelay, baseDelay, maxBackoff),
              s3MultiplePartUploadMaxRetries,
              false);
      ClientConfiguration clientConfig =
          new ClientConfiguration()
              .withRetryPolicy(retryPolicy)
              .withMaxErrorRetry(s3MultiplePartUploadMaxRetries);
      AmazonS3ClientBuilder builder =
          AmazonS3ClientBuilder.standard()
              .withCredentials(getCredentialsProvider(binding, conf))
              .withClientConfiguration(clientConfig);
      // for MinIO
      String endpoint = conf.get(Constants.ENDPOINT);
      if (!StringUtils.isEmpty(endpoint)) {
        builder =
            builder
                .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                        endpoint, conf.get(Constants.AWS_REGION)))
                .withPathStyleAccessEnabled(conf.getBoolean(Constants.PATH_STYLE_ACCESS, false));
      } else {
        builder = builder.withRegion(conf.get(Constants.AWS_REGION));
      }
      this.s3Client = builder.build();
    }

    @Override
    public void close() {
      if (s3Client != null) {
        s3Client.shutdown();
      }
    }
  }

  public S3MultipartUploadHandler(AutoCloseable sharedState, String key) {
    this.sharedState = (S3MultipartUploadHandlerSharedState) sharedState;
    this.key = key;
  }

  @Override
  public void startUpload() {
    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(sharedState.bucketName, key);
    InitiateMultipartUploadResult initResponse =
        sharedState.s3Client.initiateMultipartUpload(initRequest);
    this.uploadId = initResponse.getUploadId();
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
          new UploadPartRequest()
              .withBucketName(sharedState.bucketName)
              .withKey(key)
              .withUploadId(uploadId)
              .withPartNumber(partNumber)
              .withInputStream(inStream)
              .withPartSize(partSize)
              .withLastPart(finalFlush);
      sharedState.s3Client.uploadPart(uploadRequest);
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
    List<PartETag> partETags = new ArrayList<>();
    ListPartsRequest listPartsRequest = new ListPartsRequest(sharedState.bucketName, key, uploadId);
    PartListing partListing;
    do {
      partListing = sharedState.s3Client.listParts(listPartsRequest);
      for (PartSummary part : partListing.getParts()) {
        partETags.add(new PartETag(part.getPartNumber(), part.getETag()));
      }
      listPartsRequest.setPartNumberMarker(partListing.getNextPartNumberMarker());
    } while (partListing.isTruncated());
    if (partETags.size() == 0) {
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
    ProgressListener progressListener =
        progressEvent -> {
          logger.debug(
              "key {} uploadId {} progress event type {} transferred {} bytes",
              key,
              uploadId,
              progressEvent.getEventType(),
              progressEvent.getBytesTransferred());
        };

    CompleteMultipartUploadRequest compRequest =
        new CompleteMultipartUploadRequest(sharedState.bucketName, key, uploadId, partETags)
            .withGeneralProgressListener(progressListener);
    CompleteMultipartUploadResult compResult = null;
    for (int attempt = 1; attempt <= sharedState.s3MultiplePartUploadMaxRetries; attempt++) {
      try {
        compResult = sharedState.s3Client.completeMultipartUpload(compRequest);
        break;
      } catch (AmazonClientException e) {
        if (attempt == sharedState.s3MultiplePartUploadMaxRetries
            || !PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION.shouldRetry(null, e, attempt)) {
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
        compResult.getLocation());
  }

  @Override
  public void abort() {
    AbortMultipartUploadRequest abortMultipartUploadRequest =
        new AbortMultipartUploadRequest(sharedState.bucketName, key, uploadId);
    sharedState.s3Client.abortMultipartUpload(abortMultipartUploadRequest);
  }

  @Override
  public void close() {}

  static AWSCredentialProviderList getCredentialsProvider(URI binding, Configuration conf)
      throws IOException {
    return S3AUtils.createAWSCredentialProviderSet(binding, conf);
  }
}
