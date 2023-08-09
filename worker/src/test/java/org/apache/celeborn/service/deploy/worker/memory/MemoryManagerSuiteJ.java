package org.apache.celeborn.service.deploy.worker.memory;

import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;

public class MemoryManagerSuiteJ {

  @Test
  public void testInitMemoryManagerWithInvalidConfig() {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE(), "0.9");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE(), "0.95");
    try {
      MemoryManager.initialize(conf);
      // should throw exception before here
      Assert.fail("MemoryManager initialize should throw exception with invalid configuration");
    } catch (IllegalArgumentException iae) {
      Assert.assertEquals(
          iae.getMessage(),
          String.format(
              "Invalid config, " + "{} should be greater than {}",
              CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE().key(),
              CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE().key()));
    } catch (Exception e) {
      Assert.fail("With unexpected exception" + e);
    }
  }
}
