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

package org.apache.ratis.cli.sh;

import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.ratis.shell.cli.sh.RatisShell;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.RatisBaseSuiteJ;

public class CelebornRatisShellTest extends RatisBaseSuiteJ {
  @Test
  public void testLoadDefaultCelebornRaftProperties() {

    ScheduledExecutorService executor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("test celeborn ratis shess");
    try {
      executor
          .submit(
              new Runnable() {
                @Override
                public void run() {
                  RatisShell shell = new RatisShell(System.out);
                  shell.run(
                      "election",
                      "transfer",
                      "-peers",
                      "ip-xx-xx-xx-001:9872,ip-xMx-xx-xx-002:9872,ip-xx-xx-xx-003:9872",
                      "-groupid",
                      UUID.randomUUID().toString(),
                      "-address",
                      "ip-xx-xx-xx-003:9872");
                }
              })
          .get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      Assert.assertEquals(
          RaftUtils.testProperties.get(RaftConfigKeys.Rpc.TYPE_KEY).toLowerCase(Locale.ROOT),
          new CelebornConf().get(CelebornConf.HA_MASTER_RATIS_RPC_TYPE()).toLowerCase(Locale.ROOT));
    }
  }
}
