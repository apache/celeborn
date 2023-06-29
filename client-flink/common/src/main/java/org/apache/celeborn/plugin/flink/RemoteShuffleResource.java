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

package org.apache.celeborn.plugin.flink;

public class RemoteShuffleResource implements ShuffleResource {

  private static final long serialVersionUID = 6497939083185255973L;

  private final String metaServiceHost;
  private final int metaServicePort;
  private final long metaServiceTimestamp;
  private ShuffleResourceDescriptor shuffleResourceDescriptor;

  public RemoteShuffleResource(
      String metaServiceHost,
      int metaServicePort,
      long metaServiceTimestamp,
      ShuffleResourceDescriptor remoteShuffleDescriptor) {
    this.metaServiceHost = metaServiceHost;
    this.metaServicePort = metaServicePort;
    this.metaServiceTimestamp = metaServiceTimestamp;
    this.shuffleResourceDescriptor = remoteShuffleDescriptor;
  }

  @Override
  public ShuffleResourceDescriptor getMapPartitionShuffleDescriptor() {
    return shuffleResourceDescriptor;
  }

  public String getMetaServiceHost() {
    return metaServiceHost;
  }

  public int getMetaServicePort() {
    return metaServicePort;
  }

  public long getMetaServiceTimestamp() {
    return metaServiceTimestamp;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("RemoteShuffleResource{");
    sb.append("metaServiceHost='").append(metaServiceHost).append('\'');
    sb.append(", metaServicePort=").append(metaServicePort);
    sb.append(", metaServiceTimestamp=").append(metaServiceTimestamp);
    sb.append(", shuffleResourceDescriptor=").append(shuffleResourceDescriptor);
    sb.append('}');
    return sb.toString();
  }
}
