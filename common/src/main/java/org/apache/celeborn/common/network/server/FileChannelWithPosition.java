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

package org.apache.celeborn.common.network.server;

import java.nio.channels.FileChannel;

public class FileChannelWithPosition {
  private FileChannel channel;
  private long position;

  public FileChannelWithPosition(FileChannel channel) {
    this.channel = channel;
    this.position = 0;
  }

  public FileChannel getChannel() {
    return channel;
  }

  public void setChannel(FileChannel channel) {
    this.channel = channel;
  }

  public long getPosition() {
    return position;
  }

  public void setPosition(long position) {
    this.position = position;
  }

  public void incrementPosition(long delta) {
    this.position += delta;
  }
}
