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

package org.apache.celeborn.common.network.protocol;

public final class ChunkFetchFailureUtils {
  private static final String ERROR_CODE_PREFIX = "[CELEBORN_CHUNK_FETCH_ERROR_CODE=";
  private static final String ERROR_CODE_SUFFIX = "]";

  private ChunkFetchFailureUtils() {}

  public enum ErrorCode {
    STREAM_NOT_REGISTERED
  }

  public static String withErrorCode(ErrorCode errorCode, String message) {
    return ERROR_CODE_PREFIX + errorCode.name() + ERROR_CODE_SUFFIX + " " + message;
  }

  public static ErrorCode getErrorCode(String message) {
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

  public static String getErrorMessage(String message) {
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
