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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

public class CelebornShuffle extends Shuffle {
  public CelebornShuffle(
      InputContext inputContext,
      Configuration conf,
      int numInputs,
      long initialMemoryAvailable,
      ApplicationAttemptId applicationAttemptId)
      throws IOException {
    super(inputContext, conf, numInputs, initialMemoryAvailable);

    String host = conf.get(TEZ_CELEBORN_LM_HOST);
    int port = conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
    int shuffleId = conf.getInt(TEZ_SHUFFLE_ID, -1);
    String appId = conf.get(TEZ_CELEBORN_APPLICATION_ID);
    CelebornConf celebornConf = CelebornTezUtils.fromTezConfiguration(conf);
    ShuffleClient shuffleClient =
        ShuffleClient.get(
            appId,
            host,
            port,
            celebornConf,
            new UserIdentifier(
                celebornConf.quotaUserSpecificTenant(), celebornConf.quotaUserSpecificUserName()),
            null);

    long startTime = (long) getParentPrivateField(this, "startTime");
    CompressionCodec codec = (CompressionCodec) getParentPrivateField(this, "codec");
    boolean ifileReadAhead = (boolean) getParentPrivateField(this, "ifileReadAhead");
    int ifileReadAheadLength = (int) getParentPrivateField(this, "ifileReadAheadLength");
    String sourceDestNameTrimmed = (String) getParentPrivateField(this, "sourceDestNameTrimmed");
    CelebornScheduler scheduler =
        new CelebornScheduler(
            inputContext,
            conf,
            numInputs,
            this,
            shuffleClient,
            merger,
            merger,
            startTime,
            codec,
            ifileReadAhead,
            ifileReadAheadLength,
            sourceDestNameTrimmed,
            shuffleId,
            applicationAttemptId);
    ShuffleInputEventHandlerOrderedGrouped eventHandler =
        new ShuffleInputEventHandlerOrderedGrouped(
            inputContext, scheduler, ShuffleUtils.isTezShuffleHandler(conf));
    setParentPrivateField(this, "scheduler", scheduler);
    setParentPrivateField(this, "eventHandler", eventHandler);
  }
}
