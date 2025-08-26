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

package org.apache.celeborn.common.network.sasl.registration;

import static org.junit.Assert.*;

import org.junit.Test;

import org.apache.celeborn.common.client.MasterNotLeaderException;
import org.apache.celeborn.common.exception.CelebornException;

public class RegistrationClientBootstrapSuiteJ {

  @Test
  public void testProcessMasterNotLeaderException() {
    testProcessMasterNotLeaderExceptionImpl(
        new CelebornException(
            "Exception thrown in awaitResult",
            new MasterNotLeaderException("foo.linkedin.com:1234", "leader is not present", null)),
        true);
    testProcessMasterNotLeaderExceptionImpl(
        new CelebornException(
            "Exception thrown in awaitResult",
            new MasterNotLeaderException("abc.linkedin.com:1234", "abc.linkedin.com:567", null)),
        true);
    testProcessMasterNotLeaderExceptionImpl(
        new CelebornException(
            "Exception thrown in awaitResult",
            new MasterNotLeaderException(
                "abc.linkedin.com:1234", "abc.linkedin.com:567", new Exception("dummy"))),
        true);
    testProcessMasterNotLeaderExceptionImpl(
        new CelebornException(
            "Exception thrown in awaitResult, not MasterNotLeaderException",
            new Exception("Nested")),
        false);
  }

  private void testProcessMasterNotLeaderExceptionImpl(Exception ex, boolean shouldMatch) {
    boolean matched =
        RegistrationClientBootstrap.processMasterNotLeaderException(ex)
            instanceof MasterNotLeaderException;

    assertEquals(shouldMatch, matched);
  }
}