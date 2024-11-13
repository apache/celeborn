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
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;

import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler;
import org.apache.celeborn.server.common.service.mpu.bean.AWSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class S3MultipartUploadHandler implements MultipartUploadHandler {

  private static final Logger logger = LoggerFactory.getLogger(S3MultipartUploadHandler.class);

  private final AWSCredentials awsCredentials;
  private String uploadId;
  private AmazonS3 s3Client;
  private String key;

  public S3MultipartUploadHandler(AWSCredentials awsCredentials, String key) {
    this.awsCredentials = awsCredentials;
    BasicAWSCredentials basicAWSCredentials =
        new BasicAWSCredentials(awsCredentials.getS3AccessKey(), awsCredentials.getS3SecretKey());
    ClientConfiguration clientConfig = new ClientConfiguration()
            .withRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(5))
            .withMaxErrorRetry(5);
    this.s3Client =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
            .withRegion(awsCredentials.getS3EndpointRegion())
            .withClientConfiguration(clientConfig)
            .build();
    this.key = key;
  }

  @Override
  public void startUpload() {
    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(awsCredentials.getBucketName(), key);
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
    this.uploadId = initResponse.getUploadId();
  }

  @Override
  public void putPart(InputStream inputStream, Integer partNumber, Boolean finalFlush) throws IOException {
    try {
      int partSize = inputStream.available();
      if (partSize == 0) {
        logger.warn("key {} uploadId {} part size is 0 for part number {} finalFlush {}", key, uploadId, partNumber, finalFlush);
        return;
      }
      UploadPartRequest uploadRequest =
          new UploadPartRequest()
              .withBucketName(awsCredentials.getBucketName())
              .withKey(key)
              .withUploadId(uploadId)
              .withPartNumber(partNumber)
              .withInputStream(inputStream)
              .withPartSize(partSize)
              .withLastPart(finalFlush);
      s3Client.uploadPart(uploadRequest);
      logger.warn("key {} uploadId {} part number {} uploaded with size {} finalFlush {}",key, uploadId, partNumber, partSize, finalFlush);
    } catch (RuntimeException e) {
      logger.error("Failed to upload part", e);
      throw e;
    } catch (IOException e) {
      logger.error("Failed to upload part", e);
      throw e;
    }
  }

  @Override
  public void complete() {
    List<PartETag> partETags = new ArrayList<>();
    ListPartsRequest listPartsRequest = new ListPartsRequest(awsCredentials.getBucketName(), key, uploadId);
    PartListing partListing;
    do {
      partListing = s3Client.listParts(listPartsRequest);
      for (PartSummary part : partListing.getParts()) {
        partETags.add(new PartETag(part.getPartNumber(), part.getETag()));
      }
      listPartsRequest.setPartNumberMarker(partListing.getNextPartNumberMarker());
    } while (partListing.isTruncated());
    if (partETags.size() == 0){
      logger.debug("bucket {} key {} uploadId {} has no parts uploaded, aborting upload", awsCredentials.getBucketName(), key, uploadId);
      abort();
      logger.debug("bucket {} key {} upload completed with size {}", awsCredentials.getBucketName(), key, 0);
      return;
    }
    ProgressListener progressListener = progressEvent -> {
      logger.debug("key {} uploadId {} progress event type {} transferred {} bytes", key, uploadId, progressEvent.getEventType(), progressEvent.getBytesTransferred());
    };

      CompleteMultipartUploadRequest compRequest =
              new CompleteMultipartUploadRequest(
                      awsCredentials.getBucketName(), key, uploadId, partETags)
                      .withGeneralProgressListener(progressListener);
      CompleteMultipartUploadResult compResult = s3Client.completeMultipartUpload(compRequest);
      logger.debug("bucket {} key {} uploadId {} upload completed location is in {} ", awsCredentials.getBucketName(), key, uploadId, compResult.getLocation());
  }

  @Override
  public void abort() {
    AbortMultipartUploadRequest abortMultipartUploadRequest =
        new AbortMultipartUploadRequest(awsCredentials.getBucketName(), key, uploadId);
    s3Client.abortMultipartUpload(abortMultipartUploadRequest);
  }

  @Override
  public void close() {
    if (s3Client != null) {
      s3Client.shutdown();
    }
  }
}
