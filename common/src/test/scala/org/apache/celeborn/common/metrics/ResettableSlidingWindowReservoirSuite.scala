package org.apache.celeborn.common.metrics

import org.apache.celeborn.CelebornFunSuite

class ResettableSlidingWindowReservoirSuite extends CelebornFunSuite {
  test("test reset ResettableSlidingWindowReservoir") {
    val reservoir = new ResettableSlidingWindowReservoir(10)
    0 until 20 foreach (idx => reservoir.update(idx))
    val snapshot = reservoir.getSnapshot
    assert(snapshot.getValues.length == 10)
    reservoir.reset()
    val snapshot2 = reservoir.getSnapshot
    assert(snapshot2.getValues.length == 0)
    0 until 5 foreach (idx => reservoir.update(1))
    val snapshot3 = reservoir.getSnapshot
    assert(snapshot3.getValues.length == 5)
  }
}
