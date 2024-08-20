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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3MultipartUploader {

  private static final Logger logger = LoggerFactory.getLogger(S3MultipartUploader.class);

  private final AWSCredentials awsCredentials;
  private List<PartETag> partETags;
  private String uploadId;
  private AmazonS3 s3Client;
  private String key;

  public S3MultipartUploader(AWSCredentials awsCredentials, String key) {
    this.awsCredentials = awsCredentials;
    BasicAWSCredentials basicAWSCredentials =
        new BasicAWSCredentials(awsCredentials.getS3AccessKey(), awsCredentials.getS3AccessKey());
    this.s3Client =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
            .withRegion(awsCredentials.getS3EndpointRegion())
            .build();
    this.partETags = new ArrayList<PartETag>();
    this.key = key;
  }

  public String startUpload() {
    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(awsCredentials.getBucketName(), key);
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
    uploadId = initResponse.getUploadId();
    return uploadId;
  }

  public void putPart(Path filePath, InputStream inputStream, long lengthInBytes, int partNumber) {
    try {
      UploadPartRequest uploadRequest =
          new UploadPartRequest()
              .withBucketName(awsCredentials.getBucketName())
              .withKey(filePath.toString())
              .withUploadId(uploadId)
              .withPartNumber(partNumber)
              .withInputStream(inputStream)
              .withPartSize(lengthInBytes);

      UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
      partETags.add(uploadResult.getPartETag());
    } catch (RuntimeException e) {
      logger.error("Failed to upload part", e);
    }
  }

  public void complete() {
    CompleteMultipartUploadRequest compRequest =
        new CompleteMultipartUploadRequest(
            awsCredentials.getBucketName(), key, uploadId, partETags);
    s3Client.completeMultipartUpload(compRequest);
  }

  public void abort() {
    AbortMultipartUploadRequest abortMultipartUploadRequest =
        new AbortMultipartUploadRequest(awsCredentials.getBucketName(), key, uploadId);
    s3Client.abortMultipartUpload(abortMultipartUploadRequest);
  }
}
