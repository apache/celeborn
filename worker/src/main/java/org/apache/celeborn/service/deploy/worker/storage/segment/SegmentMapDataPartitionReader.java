/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.storage.segment;

import io.netty.channel.Channel;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.service.deploy.worker.storage.MapDataPartitionReader;

import java.util.concurrent.ExecutorService;

public class SegmentMapDataPartitionReader extends MapDataPartitionReader {

    public SegmentMapDataPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            FileInfo fileInfo,
            long streamId,
            Channel associatedChannel,
            ExecutorService readExecutor,
            Runnable recycleStream) {
        super(
                startPartitionIndex,
                endPartitionIndex,
                fileInfo,
                streamId,
                associatedChannel,
                recycleStream);
    }


    public void updateSegmentId() {
    }

    public void notifyRequiredSegmentId(int segmentId) {
    }
}
