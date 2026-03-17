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

package org.apache.celeborn.common.network.client;

import java.io.IOException;

import org.apache.celeborn.common.network.protocol.StreamChunkSlice;

/** General exception caused by a remote exception while fetching a chunk. */
public class ChunkFetchFailureException extends IOException {
  private static final String ERROR_CODE_PREFIX = "[CELEBORN_CHUNK_FETCH_ERROR_CODE=";
  private static final String ERROR_CODE_SUFFIX = "]";

  public enum ErrorCode {
    STREAM_NOT_REGISTERED
  }

  private final ErrorCode errorCode;

  public ChunkFetchFailureException(String errorMsg, Throwable cause) {
    super(errorMsg, cause);
    this.errorCode = null;
  }

  public ChunkFetchFailureException(String errorMsg) {
    super(errorMsg);
    this.errorCode = null;
  }

  public ChunkFetchFailureException(ErrorCode errorCode, String errorMsg, Throwable cause) {
    super(errorMsg, cause);
    this.errorCode = errorCode;
  }

  public ChunkFetchFailureException(ErrorCode errorCode, String errorMsg) {
    super(errorMsg);
    this.errorCode = errorCode;
  }

  public ChunkFetchFailureException(StreamChunkSlice streamChunkSlice, String errorString) {
    super("Failure while fetching " + streamChunkSlice + ": " + decodeErrorMessage(errorString));
    this.errorCode = decodeErrorCode(errorString);
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public String toChunkFetchFailureMessage() {
    if (errorCode == null) {
      return getMessage();
    }
    return encodeErrorCode(errorCode, getMessage());
  }

  private static String encodeErrorCode(ErrorCode errorCode, String message) {
    return ERROR_CODE_PREFIX + errorCode.name() + ERROR_CODE_SUFFIX + " " + message;
  }

  private static ErrorCode decodeErrorCode(String message) {
    if (message == null || !message.startsWith(ERROR_CODE_PREFIX)) {
      return null;
    }

    int suffixIndex = message.indexOf(ERROR_CODE_SUFFIX, ERROR_CODE_PREFIX.length());
    if (suffixIndex < 0) {
      return null;
    }

    String errorCodeName = message.substring(ERROR_CODE_PREFIX.length(), suffixIndex);
    try {
      return ErrorCode.valueOf(errorCodeName);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static String decodeErrorMessage(String message) {
    if (message == null) {
      return null;
    }

    int suffixIndex = message.indexOf(ERROR_CODE_SUFFIX, ERROR_CODE_PREFIX.length());
    if (!message.startsWith(ERROR_CODE_PREFIX) || suffixIndex < 0) {
      return message;
    }

    int messageStartIndex = suffixIndex + ERROR_CODE_SUFFIX.length();
    if (messageStartIndex < message.length() && message.charAt(messageStartIndex) == ' ') {
      messageStartIndex++;
    }
    return message.substring(messageStartIndex);
  }
}
