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

package org.apache.celeborn.common.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.client.ChunkFetchFailureException;
import org.apache.celeborn.common.network.protocol.ChunkFetchFailureUtils.ErrorCode;

public class ExceptionUtilsSuiteJ {
  @Test
  public void staleStreamFetchFailureIsDetectedAndNotClassifiedAsCritical() {
    CelebornIOException exception =
        new CelebornIOException(
            "Fetch chunk 0 of shuffle key app-1-1 failed.",
            new ChunkFetchFailureException(
                ErrorCode.STREAM_NOT_REGISTERED,
                "Stream 123 is not registered with worker. "
                    + "This can happen if the worker was restart recently."));

    assertTrue(ExceptionUtils.isStaleStreamChunkFetchFailure(exception));
    assertFalse(Utils$.MODULE$.isCriticalCauseForFetch(exception));
  }

  @Test
  public void staleStreamFetchFailureLegacyMessageIsStillDetected() {
    CelebornIOException exception =
        new CelebornIOException(
            "Fetch chunk 0 of shuffle key app-1-1 failed.",
            new ChunkFetchFailureException(
                "Stream 123 is not registered with worker. "
                    + "This can happen if the worker was restart recently."));

    assertTrue(ExceptionUtils.isStaleStreamChunkFetchFailure(exception));
    assertFalse(Utils$.MODULE$.isCriticalCauseForFetch(exception));
  }

  @Test
  public void timeoutFetchFailureRemainsCritical() {
    CelebornIOException exception =
        new CelebornIOException(
            "Fetch chunk 0 of shuffle key app-1-1 failed.",
            new java.io.IOException(new TimeoutException("timed out")));

    assertFalse(ExceptionUtils.isStaleStreamChunkFetchFailure(exception));
    assertTrue(Utils$.MODULE$.isCriticalCauseForFetch(exception));
  }
}
