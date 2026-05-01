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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Locale;

import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.exception.PartitionUnRetryAbleException;

public class ExceptionUtils {

  private static final String MASTER_NOT_LEADER_EXCEPTION =
      "org.apache.celeborn.common.client.MasterNotLeaderException";
  private static final String RATIS_NOT_LEADER_EXCEPTION =
      "org.apache.ratis.protocol.exceptions.NotLeaderException";

  public static void wrapAndThrowIOException(Exception exception) throws IOException {
    if (exception instanceof CelebornIOException) {
      throw (CelebornIOException) exception;
    } else if (exception instanceof IOException) {
      throw new CelebornIOException(exception);
    } else {
      throw new CelebornIOException(exception.getMessage(), exception);
    }
  }

  public static Throwable wrapIOExceptionToUnRetryable(Throwable throwable) {
    if (throwable instanceof IOException) {
      return new PartitionUnRetryAbleException(throwable.getMessage(), throwable);
    } else {
      return throwable;
    }
  }

  public static IOException wrapThrowableToIOException(Throwable throwable) {
    if (throwable instanceof IOException) {
      return (IOException) throwable;
    } else {
      return new IOException(throwable.getMessage(), throwable);
    }
  }

  public static String stringifyException(Throwable exception) {
    if (exception == null) {
      return "(null)";
    }

    try {
      StringWriter stm = new StringWriter();
      PrintWriter wrt = new PrintWriter(stm);
      exception.printStackTrace(wrt);
      wrt.close();
      return stm.toString();
    } catch (Throwable throwable) {
      return exception.getClass().getName() + " (error while printing stack trace)";
    }
  }

  public static boolean isMasterNotLeader(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      String className = current.getClass().getName();
      if (MASTER_NOT_LEADER_EXCEPTION.equals(className)
          || RATIS_NOT_LEADER_EXCEPTION.equals(className)) {
        return true;
      }
      if (isMasterNotLeaderMessage(current.getMessage())) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  public static String getRootCauseMessage(Throwable throwable) {
    if (throwable == null) {
      return "(null)";
    }

    Throwable rootCause = throwable;
    while (rootCause.getCause() != null) {
      rootCause = rootCause.getCause();
    }
    String message = rootCause.getMessage();
    if (message == null || message.isEmpty()) {
      return rootCause.getClass().getName();
    }
    int lineSeparatorIndex = message.indexOf('\n');
    return lineSeparatorIndex >= 0 ? message.substring(0, lineSeparatorIndex) : message;
  }

  public static String getMasterNotLeaderMessage(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      String className = current.getClass().getName();
      if (MASTER_NOT_LEADER_EXCEPTION.equals(className)
          || RATIS_NOT_LEADER_EXCEPTION.equals(className)
          || isMasterNotLeaderMessage(current.getMessage())) {
        return className + ": " + firstLineMessage(current);
      }
      current = current.getCause();
    }
    return getRootCauseMessage(throwable);
  }

  private static String firstLineMessage(Throwable throwable) {
    String message = throwable.getMessage();
    if (message == null || message.isEmpty()) {
      return throwable.getClass().getName();
    }
    int lineSeparatorIndex = message.indexOf('\n');
    return lineSeparatorIndex >= 0 ? message.substring(0, lineSeparatorIndex) : message;
  }

  private static boolean isMasterNotLeaderMessage(String message) {
    if (message == null) {
      return false;
    }

    String lowerMessage = message.toLowerCase(Locale.ROOT);
    return message.contains(MASTER_NOT_LEADER_EXCEPTION)
        || message.contains(RATIS_NOT_LEADER_EXCEPTION)
        || (lowerMessage.contains("is not the leader")
            && lowerMessage.contains("suggested leader"));
  }

  public static boolean connectFail(String message) {
    return (message.startsWith("Connection from ") && message.endsWith(" closed"))
        || (message.equals("Connection reset by peer"))
        || (message.startsWith("Failed to send request "));
  }
}
