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

import org.apache.celeborn.common.exception.CelebornIOException;

public class ExceptionUtils {

  public static void wrapAndThrowIOException(Exception exception) throws IOException {
    if (exception instanceof CelebornIOException) {
      throw (CelebornIOException) exception;
    } else if (exception instanceof IOException) {
      throw new CelebornIOException(exception);
    } else {
      throw new CelebornIOException(exception.getMessage(), exception);
    }
  }

  public static String summaryErrorMessageStack(Throwable t) {
    StringBuilder sb = new StringBuilder();
    do {
      if (sb.length() != 0) {
        sb.append(" -> ");
      }
      sb.append("[").append(t.getClass().getName()).append(": ").append(t.getMessage()).append("]");
    } while ((t = t.getCause()) != null);
    return sb.toString();
  }
}
