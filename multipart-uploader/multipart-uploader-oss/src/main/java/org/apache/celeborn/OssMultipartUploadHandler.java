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

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.event.ProgressListener;
import com.aliyun.oss.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler;

public class OssMultipartUploadHandler implements MultipartUploadHandler {

  private static final Logger log = LoggerFactory.getLogger(OssMultipartUploadHandler.class);

  private final String bucketName;
  private final String key;
  private final OSS ossClient;

  private String uploadId;

  public OssMultipartUploadHandler(
      String endpoint, String bucketName, String accessKey, String secretKey, String key) {
    this.bucketName = bucketName;
    this.ossClient = new OSSClientBuilder().build(endpoint, accessKey, secretKey);
    this.key = key;
  }

  @Override
  public void startUpload() {
    uploadId =
        ossClient
            .initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key))
            .getUploadId();
  }

  @Override
  public void putPart(InputStream inputStream, Integer partNumber, Boolean finalFlush)
      throws IOException {
    try (InputStream inStream = inputStream) {
      int partSize = inStream.available();
      if (partSize == 0) {
        log.debug(
            "key {} uploadId {} part size is 0 for part number {} finalFlush {}",
            key,
            uploadId,
            partNumber,
            finalFlush);
        return;
      }
      ossClient.uploadPart(
          new UploadPartRequest(bucketName, key, uploadId, partNumber, inStream, partSize));
      log.debug(
          "key {} uploadId {} part number {} uploaded with size {} finalFlush {}",
          key,
          uploadId,
          partNumber,
          partSize,
          finalFlush);
    } catch (RuntimeException | IOException e) {
      log.error("Failed to upload part", e);
      throw e;
    }
  }

  @Override
  public void complete() {
    List<PartETag> partETags = new ArrayList<>();
    ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName, key, uploadId);
    PartListing partListing;
    do {
      partListing = ossClient.listParts(listPartsRequest);
      for (PartSummary part : partListing.getParts()) {
        partETags.add(new PartETag(part.getPartNumber(), part.getETag()));
      }
      listPartsRequest.setPartNumberMarker(partListing.getNextPartNumberMarker());
    } while (partListing.isTruncated());
    if (partETags.size() == 0) {
      log.debug(
          "bucket {} key {} uploadId {} has no parts uploaded, aborting upload",
          bucketName,
          key,
          uploadId);
      abort();
      log.debug("bucket {} key {} upload completed with size {}", bucketName, key, 0);
      return;
    }
    ProgressListener progressListener =
        event ->
            log.debug(
                "key {} uploadId {} progress event type {} transferred {} bytes",
                key,
                uploadId,
                event.getEventType(),
                event.getBytes());

    CompleteMultipartUploadResult compResult =
        ossClient.completeMultipartUpload(
            new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags)
                .withProgressListener(progressListener));
    log.debug(
        "bucket {} key {} uploadId {} upload completed location is in {} ",
        bucketName,
        key,
        uploadId,
        compResult.getLocation());
  }

  @Override
  public void abort() {
    ossClient.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
  }

  @Override
  public void close() {
    if (ossClient != null) {
      ossClient.shutdown();
    }
  }
}
