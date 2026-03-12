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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandlerSharedState;

public class S3MultipartUploadHandlerSuite {

  @Test
  public void completeUsesLocallyTrackedPartEtags() throws Exception {
    AmazonS3 s3Client = mock(AmazonS3.class);

    InitiateMultipartUploadResult initResponse = new InitiateMultipartUploadResult();
    initResponse.setUploadId("upload-id");
    when(s3Client.initiateMultipartUpload(any())).thenReturn(initResponse);
    when(s3Client.uploadPart(any()))
        .thenAnswer(
            invocation -> {
              UploadPartRequest request = invocation.getArgument(0);
              UploadPartResult result = new UploadPartResult();
              result.setPartNumber(request.getPartNumber());
              result.setETag("etag-" + request.getPartNumber());
              return result;
            });
    when(s3Client.completeMultipartUpload(any())).thenReturn(new CompleteMultipartUploadResult());

    MultipartUploadHandlerSharedState sharedState =
        new MultipartUploadHandlerSharedState(s3Client, "bucket", 3, 10, 100, () -> {});
    S3MultipartUploadHandler handler = new S3MultipartUploadHandler(sharedState, "key");

    handler.putPart(new ByteArrayInputStream(new byte[] {2}), 2, false);
    handler.putPart(new ByteArrayInputStream(new byte[] {1}), 1, false);
    handler.complete();

    ArgumentCaptor<CompleteMultipartUploadRequest> requestCaptor =
        ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
    verify(s3Client).completeMultipartUpload(requestCaptor.capture());
    verify(s3Client, never()).listParts(any(ListPartsRequest.class));

    List<PartETag> partETags = requestCaptor.getValue().getPartETags();
    assertEquals(2, partETags.size());
    assertEquals(1, partETags.get(0).getPartNumber());
    assertEquals("etag-1", partETags.get(0).getETag());
    assertEquals(2, partETags.get(1).getPartNumber());
    assertEquals("etag-2", partETags.get(1).getETag());
  }
}
