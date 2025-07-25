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

package org.apache.celeborn.common.network.util;

import java.util.concurrent.atomic.AtomicLong;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;

public final class NonEventLoopEventExecutorChooserFactory implements EventExecutorChooserFactory {
  public static final NonEventLoopEventExecutorChooserFactory INSTANCE =
      new NonEventLoopEventExecutorChooserFactory();

  private NonEventLoopEventExecutorChooserFactory() {}

  @Override
  public EventExecutorChooser newChooser(EventExecutor[] executors) {
    return new NonEventLoopEventExecutorChooser(executors);
  }

  private static final class NonEventLoopEventExecutorChooser implements EventExecutorChooser {
    private final AtomicLong idx = new AtomicLong();
    private final EventExecutor[] executors;

    NonEventLoopEventExecutorChooser(EventExecutor[] executors) {
      this.executors = executors;
    }

    @Override
    public EventExecutor next() {
      int index = (int) (idx.getAndIncrement() % executors.length);
      EventExecutor executor = executors[index];
      if (executor.inEventLoop()) {
        int nextIndex = (index + 1) % executors.length;
        if (nextIndex == index) {
          throw new IllegalStateException("All executors are in the current event loop thread!");
        }
        executor = executors[nextIndex];
      }
      return executor;
    }
  }
}
