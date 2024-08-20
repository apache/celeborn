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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.PutObjectRequest;
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
    this.s3Client =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
            .withRegion(awsCredentials.getS3EndpointRegion())
            .build();
    this.key = key;
  }

  @Override
  public void startUpload() {
    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(awsCredentials.getBucketName(), key);
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
    uploadId = initResponse.getUploadId();
  }

  @Override
  public void putPart(InputStream inputStream, Long lengthInBytes, Integer partNumber) {
    try {
      UploadPartRequest uploadRequest =
          new UploadPartRequest()
              .withBucketName(awsCredentials.getBucketName())
              .withKey(key)
              .withUploadId(uploadId)
              .withPartNumber(partNumber)
              .withInputStream(inputStream)
              .withPartSize(lengthInBytes);
      s3Client.uploadPart(uploadRequest);
    } catch (RuntimeException e) {
      logger.error("Failed to upload part", e);
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
    if (partETags.size() == 0) {
      logger.debug("UploadId {} has no parts uploaded, aborting upload", uploadId);
      abort();
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(0);
      PutObjectRequest putRequest = new PutObjectRequest(awsCredentials.getBucketName(), key, new ByteArrayInputStream(new byte[0]), metadata);
      s3Client.putObject(putRequest);
      return;
    }
    CompleteMultipartUploadRequest compRequest =
        new CompleteMultipartUploadRequest(
            awsCredentials.getBucketName(), key, uploadId, partETags);
    logger.debug("UploadId {} upload completing and partSize is {}", uploadId, partETags.size());
    s3Client.completeMultipartUpload(compRequest);
  }

  @Override
  public void abort() {
    AbortMultipartUploadRequest abortMultipartUploadRequest =
        new AbortMultipartUploadRequest(awsCredentials.getBucketName(), key, uploadId);
    s3Client.abortMultipartUpload(abortMultipartUploadRequest);
  }
}
