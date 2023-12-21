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
