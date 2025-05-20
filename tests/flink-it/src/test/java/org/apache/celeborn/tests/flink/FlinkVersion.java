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

package org.apache.celeborn.tests.flink;

import org.apache.flink.annotation.Public;

/** All supported flink versions. */
@Public
public enum FlinkVersion {
  v1_16("1.16"),
  v1_17("1.17"),
  v1_18("1.18"),
  v1_19("1.19"),
  v1_20("1.20"),
  v2_0("2.0");

  private final String versionStr;

  FlinkVersion(String versionStr) {
    this.versionStr = versionStr;
  }

  public static FlinkVersion fromVersionStr(String versionStr) {
    switch (versionStr) {
      case "1.16":
        return v1_16;
      case "1.17":
        return v1_17;
      case "1.18":
        return v1_18;
      case "1.19":
        return v1_19;
      case "1.20":
        return v1_20;
      case "2.0":
        return v2_0;
      default:
        throw new IllegalArgumentException("Unsupported flink version: " + versionStr);
    }
  }

  @Override
  public String toString() {
    return versionStr;
  }

  public boolean isNewerOrEqualVersionThan(FlinkVersion otherVersion) {
    return this.ordinal() >= otherVersion.ordinal();
  }
}
