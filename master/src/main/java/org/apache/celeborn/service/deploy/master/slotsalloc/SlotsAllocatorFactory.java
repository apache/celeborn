package org.apache.celeborn.service.deploy.master.slotsalloc;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.SlotsAssignPolicy;

public class SlotsAllocatorFactory {
  public static SlotsAllocator createSlotsAllocator(SlotsAssignPolicy policy, CelebornConf conf) {
    switch (policy) {
      case ROUNDROBIN:
        return new RoundRobinSlotsAllocator();
      case LOADAWARE:
        return new LoadAwareSlotsAllocator(null == conf ? new CelebornConf() : conf);
      default:
        throw new IllegalArgumentException("Unsupported policy: " + policy);
    }
  }
}
