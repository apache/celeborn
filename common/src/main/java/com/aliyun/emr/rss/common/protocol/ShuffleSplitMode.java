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

package com.aliyun.emr.rss.common.protocol;

public enum ShuffleSplitMode {
  // soft means shuffle file reach split threshold and will receive data until shuffle split
  // request complete.
  soft(0),
  // hard means shuffle file reach split threshold and will stop receive data
  hard(1);

  private final byte value;

  ShuffleSplitMode(int value) {
    assert (value >= 0 && value < 256);
    this.value = (byte) value;
  }

  public final byte getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "ShuffleSplitMode{" +
             "value=" + name() +
             '}';
  }
}
