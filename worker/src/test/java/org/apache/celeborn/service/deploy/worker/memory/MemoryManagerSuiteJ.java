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

package org.apache.celeborn.service.deploy.worker.memory;

import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;

public class MemoryManagerSuiteJ {

  @Test
  public void testInitMemoryManagerWithInvalidConfig() {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE(), "0.95");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE(), "0.85");
    try {
      MemoryManager.initialize(conf);
      // should throw exception before here
      Assert.fail("MemoryManager initialize should throw exception with invalid configuration");
    } catch (IllegalArgumentException iae) {
      Assert.assertEquals(
          String.format(
              "Invalid config, {} should be greater than {}",
              CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE().key(),
              CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE().key()),
          iae.getMessage());
    } catch (Exception e) {
      Assert.fail("With unexpected exception" + e);
    }
  }
}
