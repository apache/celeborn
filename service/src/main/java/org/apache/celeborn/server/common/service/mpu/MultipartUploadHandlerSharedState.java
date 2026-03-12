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

package org.apache.celeborn.server.common.service.mpu;

import java.util.concurrent.atomic.AtomicBoolean;

public class MultipartUploadHandlerSharedState implements AutoCloseable {
  private final Object client;
  private final String bucketName;
  private final int maxRetries;
  private final int baseDelay;
  private final int maxBackoff;
  private final Runnable closeAction;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public MultipartUploadHandlerSharedState(
      Object client,
      String bucketName,
      int maxRetries,
      int baseDelay,
      int maxBackoff,
      Runnable closeAction) {
    this.client = client;
    this.bucketName = bucketName;
    this.maxRetries = maxRetries;
    this.baseDelay = baseDelay;
    this.maxBackoff = maxBackoff;
    this.closeAction = closeAction;
  }

  public Object getClient() {
    return client;
  }

  public String getBucketName() {
    return bucketName;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public int getBaseDelay() {
    return baseDelay;
  }

  public int getMaxBackoff() {
    return maxBackoff;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true) && closeAction != null) {
      closeAction.run();
    }
  }
}
