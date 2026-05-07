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

package org.apache.celeborn.common.client;

import java.io.IOException;

import javax.annotation.Nullable;

import scala.Tuple2;

import org.apache.commons.lang3.StringUtils;

public class MasterNotLeaderException extends IOException {

  private static final long serialVersionUID = -2552475565785098271L;

  private final String leaderPeer;
  private final String internalLeaderPeer;

  public static final String LEADER_NOT_PRESENTED = "leader is not present";

  public MasterNotLeaderException(
      String currentPeer, String suggestedLeaderPeer, @Nullable Throwable cause) {
    this(
        currentPeer,
        Tuple2.apply(suggestedLeaderPeer, suggestedLeaderPeer),
        Tuple2.apply(suggestedLeaderPeer, suggestedLeaderPeer),
        false,
        cause);
  }

  public MasterNotLeaderException(
      String currentPeer,
      Tuple2<String, String> suggestedLeaderPeer,
      Tuple2<String, String> suggestedInternalLeaderPeer,
      boolean bindPreferIp,
      @Nullable Throwable cause) {
    super(
        String.format(
            "Master:%s is not the leader.%s%s",
            currentPeer,
            currentPeer.equals(suggestedLeaderPeer._1)
                ? StringUtils.EMPTY
                : String.format(
                    // both (host and ip) point to the preferred address configured by bindPreferIp
                    // preserve the earlier format of Tuple._2
                    " Suggested leader is Master:(%s,%s) ((%s,%s)).",
                    bindPreferIp ? suggestedLeaderPeer._1 : suggestedLeaderPeer._2,
                    bindPreferIp ? suggestedLeaderPeer._1 : suggestedLeaderPeer._2,
                    bindPreferIp ? suggestedInternalLeaderPeer._1 : suggestedInternalLeaderPeer._2,
                    bindPreferIp ? suggestedInternalLeaderPeer._1 : suggestedInternalLeaderPeer._2),
            cause == null
                ? StringUtils.EMPTY
                : String.format(" Exception:%s.", cause.getMessage())),
        cause);
    this.leaderPeer = bindPreferIp ? suggestedLeaderPeer._1 : suggestedLeaderPeer._2;
    this.internalLeaderPeer =
        bindPreferIp ? suggestedInternalLeaderPeer._1 : suggestedInternalLeaderPeer._2;
  }

  public String getSuggestedLeaderAddress() {
    return leaderPeer;
  }

  public String getSuggestedInternalLeaderAddress() {
    return internalLeaderPeer;
  }
}
